package Spark.APIJAVA;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;

@SuppressWarnings("serial")
public class ValueComparator<K, V> implements Comparator<Tuple2<K, V>>, Serializable {
	private Comparator<V> comparator;

	public ValueComparator(Comparator<V> comparator) {
		this.comparator = comparator;
	}

	@Override
	public int compare(Tuple2<K, V> o1, Tuple2<K, V> o2) {
		return comparator.compare(o1._2(), o2._2());
	}
}