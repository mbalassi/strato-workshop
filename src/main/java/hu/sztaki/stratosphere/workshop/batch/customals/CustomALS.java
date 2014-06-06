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

import eu.stratosphere.api.java.IterativeDataSet;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.operators.DataSink;
import eu.stratosphere.api.java.tuple.Tuple3;

//Parameters: [noSubStasks] [matrix] [output] [rank] [numberOfIterations] [lambda]
public class CustomALS {

	public static void executeALS(int numSubTasks, String matrixInput, String output, int k,
                                  int numIterations, double lambda) throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// input rating matrix
		DataSet<MatrixEntry> matrixSource = env.readCsvFile(matrixInput)
				.fieldDelimiter('|').lineDelimiter("|\n").includeFields(true, true, true)
				.tupleType(MatrixEntry.class);

		// create the rowwise partition of A for machines
		DataSet<Partition<MatrixEntry>> rowPartitionA = matrixSource
				.map(new MultiplyMatrix(numSubTasks, 0)).name("MultiplyingMatrixRows");

		// create the columnwise partition of A for machines
		DataSet<Partition<MatrixEntry>> colPartitionA = matrixSource
				.map(new MultiplyMatrix(numSubTasks, 1)).name("MultiplyingMatrixColumns");

		// for creating a random matrix
		IterativeDataSet<Partition<MatrixLine>> initialQ = matrixSource
                .groupBy(1)
				.reduceGroup(new RandomMatrix(numSubTasks, k))
                .name("Create q as a random matrix")
                .iterate(numIterations);

		DataSet<Partition<MatrixLine>> p = rowPartitionA.coGroup(initialQ)
				.where(0)
				.equalTo(0)
				.with(new Iteration(numSubTasks, k, lambda, 0))
				.name("P iteration");

		DataSet<Partition<MatrixLine>> nextQ = colPartitionA.coGroup(p)
				.where(0)
				.equalTo(0)
				.with(new Iteration(numSubTasks, k, lambda, 1))
				.name("Q iteration");

		DataSet<Partition<MatrixLine>> q = initialQ.closeWith(nextQ);

		p = rowPartitionA.coGroup(q)
				.where(0)
				.equalTo(0)
				.with(new Iteration(numSubTasks, k, lambda, 0))
				.name("P iteration");

		// delete marker fields
		DataSet<MatrixLine> pOutFormat = p.groupBy(0)
				.reduceGroup(new OutputFormatter(numSubTasks)).name("P output format");

		DataSet<MatrixLine> qOutFormat = q.groupBy(0)
				.reduceGroup(new OutputFormatter(numSubTasks)).name("Q output format");

		// output
		ColumnOutputFormat pFormat = new ColumnOutputFormat(output + "/p");
		pFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);
		DataSink<MatrixLine> pSink = pOutFormat.output(pFormat);

		ColumnOutputFormat qFormat = new ColumnOutputFormat(output + "/q");
		qFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);
		DataSink<MatrixLine> qSink = qOutFormat.output(qFormat);

		env.setDegreeOfParallelism(numSubTasks);
		env.execute("CustomALS");
	}

	public static void main(String[] args) throws Exception{
		int numSubTasks = 2;
		String sampleDB2 = "file://" + CustomALS.class.getResource("/testdata/als_batch/sampledb2.csv");
		String sampleDB3 = "file://" + CustomALS.class.getResource("/testdata/als_batch/sampledb3.csv");
		String output = "file:///" + System.getProperty("user.dir") + "/als_custom_output";
		int k = 5;
		int numIterations = 3;
		double lambda = 0.1;

		CustomALS.executeALS(numSubTasks, sampleDB2, output, k, numIterations, lambda);
	}

}
