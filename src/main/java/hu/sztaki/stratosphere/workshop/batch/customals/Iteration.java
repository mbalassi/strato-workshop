/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package hu.sztaki.stratosphere.workshop.batch.customals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import Jama.Matrix;
import eu.stratosphere.api.java.functions.CoGroupFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.util.Collector;

public class Iteration extends CoGroupFunction<Partition<MatrixEntry>, Partition<MatrixLine>, Partition<MatrixLine>> {

	private int k;
	private int numOfTasks;
	private double lambda;

	private int idx;

	public Iteration(int numTasks, int k, double lambda, int index) {
		this.numOfTasks = numTasks;
		this.k = k;
		this.idx = index;
		this.lambda = lambda;
	}

	@Override
	public void coGroup(Iterator<Partition<MatrixEntry>> entries, Iterator<Partition<MatrixLine>> lines,
					   Collector<Partition<MatrixLine>> out)throws Exception {
		Map<Integer, List<Tuple2<Integer, Double>>> matrixElements =
				new HashMap<Integer, List<Tuple2<Integer, Double>>>();
		Map<Integer, double[]> vectors = new HashMap<Integer, double[]>();

		if(!lines.hasNext()){
			return;
		}

		while (entries.hasNext()) {
			MatrixEntry entry = entries.next().f1;
			addToMatrixMap(matrixElements, entry);
		}

		while(lines.hasNext()){
			MatrixLine line = lines.next().f1;
			addToVectorMap(vectors, line);
		}

		for (int recordIndex : matrixElements.keySet()) {
			//solve the linear equation system corresponding to this machine
			Matrix p = compute(recordIndex, matrixElements, vectors);

			writeOutput(p, recordIndex, out);
		}
	}

	private void writeOutput(Matrix p, int recordIndex, Collector<Partition<MatrixLine>> out) {

		double[] output_elements = new double[k];
		for (int i = 0; i < k; ++i) {
			output_elements[i] = p.get(i, 0);
		}

		for (int i = 0; i < numOfTasks; ++i) {//send new vector for all machines
			Partition<MatrixLine> output = new Partition<MatrixLine>(i, new MatrixLine(recordIndex, output_elements));
			out.collect(output);
		}


	}

	private Matrix compute(int recordIndex,
						   Map<Integer, List<Tuple2<Integer, Double>>> matrixElements, Map<Integer, double[]> vectors) {

		double[][] matrix = new double[k][k];

		double element = lambda; //Lambda-regularization

		if(lambda != 0.0) {
			for(double[] row : matrix) {
				Arrays.fill(row, element);
			}
		}

		double[][] column = new double[k][1];
		List<Tuple2<Integer, Double>> list = matrixElements.get(recordIndex);

		for (Tuple2<Integer, Double> pair : list) {
			double rating = pair.f1;
			double[] vector = vectors.get(pair.f0);
			for (int i = 0; i < k; ++i) {
				column[i][0] += rating * vector[i];
			}

			for (int i = 0; i < k; ++i) {
				for (int j = 0; j <= i; ++j) {
					matrix[i][j] += vector[i] * vector[j];
				}
			}
		}

		for (int i = 0; i < k; ++i) {
			for (int j = i + 1; j < k; ++j) {
				matrix[i][j] = matrix[j][i];
			}
		}
		Matrix a = new Matrix(matrix);
		Matrix b = new Matrix(column);
		Matrix p = a./*chol().*/solve(b);
		return p;
	}

	private void addToVectorMap(Map<Integer, double[]> vectors,
								MatrixLine line) {
		vectors.put(line.getIndex(), line.getValues());
	}

	private void addToMatrixMap(Map<Integer, List<Tuple2<Integer, Double>>> map, MatrixEntry entry) throws
			Exception {
		int recordIndex = entry.getField(idx);
		int otherIndex = entry.getField(1-idx);
		double value = entry.getEntry();

		if (map.containsKey(recordIndex)) {
			List<Tuple2<Integer, Double>> list = map.get(recordIndex);
			list.add(new Tuple2<Integer, Double>(otherIndex, value));
		} else {
			List<Tuple2<Integer, Double>> list = new ArrayList<Tuple2<Integer, Double>>();
			list.add(new Tuple2<Integer, Double>(otherIndex, value));
			map.put(recordIndex, list);
		}
	}
}
