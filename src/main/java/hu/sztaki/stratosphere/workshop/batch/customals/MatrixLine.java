package hu.sztaki.stratosphere.workshop.batch.customals;

import eu.stratosphere.api.java.tuple.Tuple2;

public class MatrixLine extends Tuple2<Integer, double[]> {
	public MatrixLine(){
		super();
	}

	public MatrixLine(int index, double[] values){
		super(index, values);
	}

	public int getIndex() { return f0; }

	public double[] getValues() { return f1; }
}
