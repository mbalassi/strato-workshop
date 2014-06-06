package hu.sztaki.stratosphere.workshop.batch.customals;

import eu.stratosphere.api.java.tuple.Tuple3;

public class MatrixEntry extends Tuple3<Integer, Integer, Double> {
	public MatrixEntry(){
		super();
	}

	public MatrixEntry(int rowIndex, int columnIndex, double value){
		super(rowIndex, columnIndex, value);
	}

	public int getRowIndex() { return f0; }

	public int getColumnIndex() { return f1; }

	public double getEntry() { return f2; }
}
