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

package hu.sztaki.stratosphere.workshop.batch.als;

import eu.stratosphere.api.java.IterativeDataSet;
import eu.stratosphere.core.fs.FileSystem;
import hu.sztaki.stratosphere.workshop.batch.outputformat.ColumnOutputFormat;

import eu.stratosphere.api.java.operators.DataSink;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.tuple.Tuple2;

public class ALS {

	public static void executeALS(int numSubTasks, String matrixInput, String output, int k,
								  int numIterations, double lambda) throws Exception{
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		//input rating matrix
		DataSet<Tuple3<Integer,Integer,Double>> matrixSource = env.readCsvFile(matrixInput)
				.fieldDelimiter('|')
				.lineDelimiter("|\n")
				.includeFields(true, true, true)
				.types(Integer.class,Integer.class,Double.class);

		//for random q matrix as input
		DataSet<Tuple2<Integer,double[]>> q = matrixSource
				.groupBy(1)
				.reduceGroup(new RandomMatrix(k))
				.name("Create q as a random matrix");

		IterativeDataSet<Tuple2<Integer, double[]>> initialQ = q.iterate(numIterations);

		DataSet<Tuple3<Integer, Integer, double[]>> multipliedQ = matrixSource.join(initialQ)
				.where(1)
				.equalTo(0)
				.with(new MultiplyVector())
				.name("Sends the columns of q with multiple keys");

		DataSet<Tuple2<Integer,double[]>> p = matrixSource.coGroup(multipliedQ)
				.where(0).equalTo(0)
				.with( new PIteration(k,lambda))
				.name("For fixed q calculates optimal p");

		DataSet<Tuple3<Integer,Integer, double[]>> multipliedP = matrixSource.join(p)
				.where(0).equalTo(0)
				.with( new MultiplyVector())
				.name("Sends the rows of p with multiple keys)");

		DataSet<Tuple2<Integer, double[]>> nextQ = matrixSource.coGroup(multipliedP)
				.where(1).equalTo(1)
				.with( new QIteration(k,lambda))
				.name("For fixed p calculates optimal q");

		q = initialQ.closeWith(nextQ);

		multipliedQ = matrixSource.join(q)
				.where(1)
				.equalTo(0)
				.with(new MultiplyVector())
				.name("Sends the columns of q with multiple keys");

		p = matrixSource.coGroup(multipliedQ)
				.where(0).equalTo(0)
				.with( new PIteration(k,lambda))
				.name("For fixed q calculates optimal p");

		//output:
		ColumnOutputFormat pFormat = new ColumnOutputFormat(output + "/p");
		pFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);
		DataSink<Tuple2<Integer,double[]>> pOut = p.output(pFormat);

		ColumnOutputFormat qFormat = new ColumnOutputFormat(output + "/q");
		qFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);
		DataSink<Tuple2<Integer,double[]>> qOut = q.output(qFormat);

		env.setDegreeOfParallelism(numSubTasks);

		env.execute("ALS");
	}

	public static void main(String[] args) throws Exception {
		int numSubTasks = 1;
		String sampleDB2 = "file://" + ALS.class.getResource("/testdata/als_batch/sampledb2.csv").getPath();
		String sampleDB3 = "file://" + ALS.class.getResource("/testdata/als_batch/sampledb3.csv").getPath();
		String output = "file:///" + System.getProperty("user.dir") + "/als_output";
		int k = 5;
		int numIterations = 3;
		double lambda = 0.1;

		executeALS(numSubTasks, sampleDB2, output, k, numIterations, lambda);
	}
}
