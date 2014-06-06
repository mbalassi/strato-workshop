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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import eu.stratosphere.util.Collector;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple5;

public class OutputFormatter extends GroupReduceFunction<Partition<MatrixLine>, MatrixLine> {

  private int numOfTasks;
  private Tuple2<Integer,double[]> output = new Tuple2();
  private Set<Integer> ids_ = new HashSet<Integer>(); 
  
  public OutputFormatter(int numTaks) {
    this.numOfTasks = numTaks;
  }
  
  @Override
  public void reduce(Iterator<Partition<MatrixLine>> records, Collector<MatrixLine> out)
      throws Exception {
    
    ids_.clear(); 
    while (records.hasNext()) {
      
      //Delete the first two marker fields of the vectors and send each vector only once
      //(Duplication of the same column do occur with several machince IDs as we sent the results for all machine in Iteration)
    }
  }

}
