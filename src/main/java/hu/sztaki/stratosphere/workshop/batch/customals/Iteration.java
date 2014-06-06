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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import Jama.Matrix;
import eu.stratosphere.api.java.functions.CoGroupFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.util.Collector;
import eu.stratosphere.api.java.tuple.Tuple5;

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

		while (entries.hasNext()) {
			Partition<MatrixEntry> partitionEntry = entries.next();
			boolean isMatrix = record.f1;
			//we sort our data according to whether it corresponds to an element of the rating matrix or a column of P or Q
			if (isMatrix) {
				addToMatrixMap(matrixElements, record);
			} else {
				addToVectorMap(vectors, record);
			}
		}

		if(vectors.isEmpty()){
			return;
		}

		for (int recordIndex : matrixElements.keySet()) {
			//solve the linear equation system corresponding to this machine
			Matrix p = compute(recordIndex, matrixElements, vectors);

			writeOutput(p, recordIndex, out);
		}
	}

	private void writeOutput(Matrix p, int recordIndex, Collector<Tuple5<Integer,Boolean,Integer,Integer,double[]>> out) {

		double[] output_elements = new double[k];
		for (int i = 0; i < k; ++i) {
			output_elements[i] = p.get(i, 0);
		}

		//TODO: set the element of the output vector and collect it with all machineIDs
		//Hint: the output has the following format: (machineID,FALSE,recordIndex,ZERO,output_elements)

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
								Partition<MatrixLine> line) {
		//TODO: add the given vector's element to the map
		//Hint: the map contains (columnID, vectorOfTheElements) pairs

	}

	private void addToMatrixMap(Map<Integer, List<MatrixEntry>> map, Partition<MatrixLine> linePartition) throws
			Exception {
		int ownIndex = linePartition.f0;
		int recordIndex = linePartition.getField(2 + idx);//if idx==0 then we update P given the columns of Q
		int otherIndex = linePartition.getField(3 - idx);//if idx==1 then we update Q given the rows of P
		double[] elements = linePartition.f4;
		double value = elements[0];

		//Hint: each IntDoublePair is a (otherIndex,value) pair. A List of these object corresponds to each vector which marked for update and has the recordIndex identifier.
		//TODO: store the incoming record's fields in the given map, but make sure there is no duplication of the data


	}
}
