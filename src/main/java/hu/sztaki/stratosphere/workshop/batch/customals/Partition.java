package hu.sztaki.stratosphere.workshop.batch.customals;

import eu.stratosphere.api.java.tuple.Tuple2;

public class Partition<T> extends Tuple2<Integer, T> {
	public Partition(){
		super();
	}

	public Partition(int partitionNumber, T value){
		super(partitionNumber, value);
	}

	public int getPartitionNumber() { return f0; }

	public T getValue() { return f1; }

}
