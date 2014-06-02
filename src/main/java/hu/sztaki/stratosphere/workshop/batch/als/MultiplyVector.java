package hu.sztaki.stratosphere.workshop.batch.als;

import java.util.Collection;
import java.util.Iterator;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;

public class MultiplyVector
		extends
		JoinFunction<Tuple3<Integer, Integer, Double>, Tuple2<Integer, double[]>, Tuple3<Integer, Integer, double[]>> {

	@Override
	public Tuple3<Integer, Integer, double[]> join(Tuple3<Integer, Integer, Double> matrixElement,
			Tuple2<Integer, double[]> columnOfQ) {

		// TODO: send the given j^th column of Q matrix with all i rowId where
		// the a_ij element is given in the A rating matrix.

		return null;

	}

}
