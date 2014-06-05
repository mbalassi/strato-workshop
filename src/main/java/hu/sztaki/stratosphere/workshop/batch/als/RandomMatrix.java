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

import java.util.Iterator;
import java.util.Random;
import java.util.Collection;

import eu.stratosphere.util.Collector;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;

public class RandomMatrix extends GroupReduceFunction<Tuple3<Integer,Integer,Double>,Tuple2<Integer,double[]>> {

  private int k;
  private Tuple2<Integer, double[]> vector = new Tuple2();
  private final Random RANDOM = new Random();
  
  public RandomMatrix(int k) {
    this.k = k; 
  }

  @Override
  public void reduce(Iterator<Tuple3<Integer,Integer,Double>> elements, Collector<Tuple2<Integer,double[]>> out)
      throws Exception {
    Tuple3<Integer,Integer,Double> element = elements.next();
    double[] vector_elements= new double[k];
   
    //TODO: generate a k length random vector for each column of Q. Hint: for faster convergence generate the elements from [1,1.5] interval.
    for (int i = 0; i < k; ++i) {
      vector_elements[i]= 1 + RANDOM.nextDouble() / 2;
    }
    
    //TODO: collect the generated columns in the given Collector.
    vector.setFields(element.f1,vector_elements);
    out.collect(vector);
  }
}
