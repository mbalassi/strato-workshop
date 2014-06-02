package hu.sztaki.stratosphere.workshop.batch.als;

import java.util.Collection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import Jama.Matrix;

import eu.stratosphere.api.java.functions.CoGroupFunction;
import eu.stratosphere.util.Collector;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;

public class QIteration
		extends
		CoGroupFunction<Tuple3<Integer, Integer, Double>, Tuple3<Integer, Integer, double[]>, Tuple2<Integer, double[]>> {

	private int k;
	private double lambda;
	private int id_;
	private Tuple2<Integer, double[]> result_ = new Tuple2();

	public QIteration(int k, double lambda) {
		this.k = k;
		this.lambda = lambda;
	}

	@Override
	public void coGroup(Iterator<Tuple3<Integer, Integer, Double>> matrixElements,
			Iterator<Tuple3<Integer, Integer, double[]>> p, Collector<Tuple2<Integer, double[]>> out) {

		double[][] matrix = new double[k][k];
		double[][] vector = new double[k][1];

		double element_ = lambda; // Regularization with Frobenius-norm

		if (lambda != 0.0) {
			for (double[] row : matrix) {
				Arrays.fill(row, element_);
			}
		}

		Map<Integer, Double> ratings = new HashMap<Integer, Double>();
		while (matrixElements.hasNext()) {
			Tuple3<Integer, Integer, Double> element = matrixElements.next();
			id_ = element.f1;
			ratings.put(element.f0, element.f2);
		}

		while (p.hasNext()) {

			// TODO: create the kxk double[][] matrix from the rows of P
			// TODO: create the kx1 vector for the linear equation system

			Matrix a = new Matrix(matrix);
			Matrix b = new Matrix(vector);
			Matrix result = a.solve(b);

			double[] result_elements = new double[k];
			for (int i = 0; i < k; ++i) {
				result_elements[i] = result.get(i, 0);
			}
			result_.setFields(id_, result_elements);
			out.collect(result_);
		}

	}
}
