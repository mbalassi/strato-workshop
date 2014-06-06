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

import java.util.Collection;
import java.util.Iterator;

import eu.stratosphere.util.Collector;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.tuple.Tuple5;

public class MultiplyMatrix extends MapFunction<Tuple3<Integer,Integer,Double>,Partition<MatrixEntry>> {

  private int numOfTasks;
  private Tuple5<Integer,Boolean,Integer,Integer,double[]> output = new Tuple5();
  private int index;
  
  public MultiplyMatrix(int numTask, int index) {
    this.numOfTasks = numTask; 
    this.index = index;
  }
  
  @Override
  public Partition<MatrixEntry> map(Tuple3<Integer,Integer, Double> record) throws Exception {

    int ownIndex = record.getField(index);//when ==0 then we do rowwise partition, when ==1 we do a columnwise partition
    
    //TODO: assign each element of the (sparse) rating matrix uniformly to a machine
    //TODO: the output vector has the (machinceIndex, TRUE, rowID, columnID, double[1]{elementValue}) format
    
    return null;
  
  }

}
