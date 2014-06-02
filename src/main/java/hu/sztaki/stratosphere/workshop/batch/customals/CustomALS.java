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

import hu.sztaki.stratosphere.workshop.batch.outputformat.ColumnOutputFormat;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.operators.DataSink;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.tuple.Tuple5;

//Parameters: [noSubStasks] [matrix] [output] [rank] [numberOfIterations] [lambda]
public class CustomALS {

	public static void main(String[] args) throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// parse job parameters
		int numSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String matrixInput = (args.length > 1 ? args[1] : "");
		String output = (args.length > 2 ? args[2] : "");
		int k = (args.length > 3 ? Integer.parseInt(args[3]) : 1);
		int iteration = (args.length > 4 ? Integer.parseInt(args[4]) : 1);
		double lambda = (args.length > 5 ? Double.parseDouble(args[5]) : 0.0);

		// input rating matrix
		DataSet<Tuple3<Integer, Integer, Double>> matrixSource = env.readCsvFile(matrixInput)
				.fieldDelimiter('|').lineDelimiter("|\n").includeFields(true, true, true)
				.types(Integer.class, Integer.class, Double.class);

		// create the rowwise partition of A for machines
		DataSet<Tuple5<Integer, Boolean, Integer, Integer, double[]>> rowPartitionA = matrixSource
				.map(new MultiplyMatrix(numSubTasks, 0)).name("MultiplyingMatrixRows");

		// create the columnwise partition of A for machines
		DataSet<Tuple5<Integer, Boolean, Integer, Integer, double[]>> colPartitionA = matrixSource
				.map(new MultiplyMatrix(numSubTasks, 1)).name("MultiplyingMatrixColumns");

		// for creating a random matrix
		DataSet<Tuple5<Integer, Boolean, Integer, Integer, double[]>> q = matrixSource.groupBy(1)
				.reduceGroup(new RandomMatrix(numSubTasks, k)).name("Create q as a random matrix");

		DataSet<Tuple5<Integer, Boolean, Integer, Integer, double[]>> p = null;

		// iteration
		for (int i = 0; i < iteration; ++i) {
			// create the dataset used in the update of p
			DataSet<Tuple5<Integer, Boolean, Integer, Integer, double[]>> dataForPIteration = rowPartitionA
					.union(q);
			// update p
			p = dataForPIteration.groupBy(0).reduceGroup(new Iteration(numSubTasks, k, lambda, 0))
					.name("P iter: " + (i + 1));

			// create the dataset used in the update of q
			DataSet<Tuple5<Integer, Boolean, Integer, Integer, double[]>> dataForQIteration = colPartitionA
					.union(p);
			// update q
			q = dataForQIteration.groupBy(0).reduceGroup(new Iteration(numSubTasks, k, lambda, 1))
					.name("Q iter: " + (i + 1));
		}

		// delete marker fields
		DataSet<Tuple2<Integer, double[]>> pOutFormat = p.groupBy(0)
				.reduceGroup(new OutputFormatter(numSubTasks)).name("P output format");

		DataSet<Tuple2<Integer, double[]>> qOutFormat = q.groupBy(0)
				.reduceGroup(new OutputFormatter(numSubTasks)).name("Q output format");

		// output
		ColumnOutputFormat pFormat = new ColumnOutputFormat(output + "/p");
		DataSink<Tuple2<Integer, double[]>> pSink = pOutFormat.output(pFormat);

		ColumnOutputFormat qFormat = new ColumnOutputFormat(output + "/q");
		DataSink<Tuple2<Integer, double[]>> qSink = qOutFormat.output(qFormat);

		env.setDegreeOfParallelism(numSubTasks);
		env.execute("CustomALS");
	}

}
