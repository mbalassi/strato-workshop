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

import java.util.Iterator;
import java.util.Collection;
import java.util.Random;

import eu.stratosphere.util.Collector;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.tuple.Tuple5;

public class RandomMatrix extends GroupReduceFunction<MatrixEntry,Partition<MatrixLine>> {

  private int k;
  private int numOfTasks;

  private double[] vector_elements;
  private final Random RANDOM = new Random();

  public RandomMatrix(int numTask, int k) {
    this.numOfTasks = numTask; 
    this.k = k;
  }

  @Override
  public void reduce(Iterator<MatrixEntry> elements, Collector<Partition<MatrixLine>> out)
   throws Exception {
    Tuple3<Integer,Integer,Double> element = elements.next();
    vector_elements = new double[k];
    
    //TODO: Generate a k length random vector for each column of Q. Hint: generate the elements from the [1,1.5] interval.
   


    //TODO: Collect the generated column of Q for all machineId. 
    //Hint: use the following format: (machineId,FALSE,columnId,ZERO,random vector of the column) 
  
  
  
  }
}
