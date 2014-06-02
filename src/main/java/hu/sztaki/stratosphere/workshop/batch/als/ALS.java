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

import hu.sztaki.stratosphere.workshop.batch.outputformat.ColumnOutputFormat;

import eu.stratosphere.api.java.operators.DataSink;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.functions.KeySelector;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple5;

//Parameters: [noSubStasks] [matrix] [output] [rank] [numberOfIterations] [lambda] 
public class ALS {

  public static void main(String[] args) throws Exception {	
    
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // parse job parameters
    int noSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
    String matrixInput = (args.length > 1 ? args[1] : "");
    String output = (args.length > 2 ? args[2] : "");
    int k = (args.length > 3 ? Integer.parseInt(args[3]) : 1);
    int iteration = (args.length > 4 ? Integer.parseInt(args[4]) : 1);
    double lambda = (args.length > 5 ? Double.parseDouble(args[5]) : 0.0);
    
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
   
    DataSet<Tuple2<Integer,double[]>> p = null;

    //iteration
    for (int i = 0; i < iteration; ++i) {
      DataSet<Tuple3<Integer,Integer,double[]>> multipliedQ = matrixSource.join(q)
	   .where(1).equalTo(0)
           .with( new MultiplyVector())
           .name("Sends the columns of q with multiple keys)");
      
      p = matrixSource.coGroup(multipliedQ)
	   .where(0).equalTo(0)
	   .with( new PIteration(k,lambda))
           .name("For fixed q calculates optimal p");
      
      DataSet<Tuple3<Integer,Integer, double[]>> multipliedP = matrixSource.join(p)
	   .where(0).equalTo(0)
	   .with( new MultiplyVector())
           .name("Sends the rows of p with multiple keys)");
 
      q = matrixSource.coGroup(multipliedP)
	   .where(1).equalTo(1)
	   .with( new QIteration(k,lambda))
           .name("For fixed p calculates optimal q");    

    }
    
    //output:
    ColumnOutputFormat pFormat = new ColumnOutputFormat(output + "/p");
    DataSink<Tuple2<Integer,double[]>> pOut = p.output(pFormat);

    ColumnOutputFormat qFormat = new ColumnOutputFormat(output + "/q");
    DataSink<Tuple2<Integer,double[]>> qOut = q.output(qFormat);

    env.setDegreeOfParallelism(noSubTasks);
    
    env.execute("ALS");
  }

}
