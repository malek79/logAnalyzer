package Spark.APIJAVA;

import java.util.Comparator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class LogAnalyzer {
	public static void main(String[] args) {
		// Create a Spark Context.
		Function2<Long, Long, Long> SUM_REDUCER = (a, b) -> a + b;

		SparkSession spark = SparkSession.builder().appName("Log Analyzer").master("local[*]").getOrCreate();

		// Load the text file into Spark.
		if (args.length == 0) {
			System.out.println("Must specify an access logs file.");
			System.exit(-1);
		}
		String logFile = args[0];
		JavaRDD<String> logLines = spark.read().textFile(logFile).toJavaRDD();

		// Convert the text log lines to ApacheAccessLog objects and
		// cache them since multiple transformations and actions
		// will be called on the data.
		JavaRDD<ApacheAccessLog> accessLogs = logLines.map(ApacheAccessLog::parseFromLogLine).cache();

		// Calculate statistics based on the content size.
		// Note how the contentSizes are cached as well since multiple actions
		// are called on that RDD.
		JavaRDD<Long> contentSizes = accessLogs.map( m -> m.getContentSize()).cache();

		System.out.println(String.format("Content Size Avg: %s, Min: %s, Max: %s",
				contentSizes.reduce((a, b) -> a + b) / contentSizes.count(), contentSizes.min(Comparator.naturalOrder()),
				contentSizes.max(Comparator.naturalOrder())));

		// Compute Response Code to Count.
		List<Tuple2<Integer, Long>> responseCodeToCount = accessLogs
				.mapToPair(log -> new Tuple2<>(log.getResponseCode(), 1L)).reduceByKey(SUM_REDUCER).take(100);
		System.out.println(String.format("Response code counts: %s", responseCodeToCount));

		List<String> ipAddresses = accessLogs.mapToPair(log -> new Tuple2<>(log.getIpAddress(), 1L))
				.reduceByKey(SUM_REDUCER).filter(tuple -> tuple._2() > 10).map(Tuple2::_1).take(100);
		System.out.println(String.format("IPAddresses > 10 times: %s", ipAddresses));

		List<Tuple2<String, Long>> topEndpoints = accessLogs.mapToPair(log -> new Tuple2<>(log.getEndpoint(), 1L))
				.reduceByKey(SUM_REDUCER).top(10, new ValueComparator<>(Comparator.<Long>naturalOrder()));
		System.out.println("Top Endpoints: " + topEndpoints);

		spark.close();
	}
}