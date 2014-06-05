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

import java.util.Collection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import Jama.Matrix;

import eu.stratosphere.util.Collector;
import eu.stratosphere.api.java.functions.CoGroupFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;

public class PIteration extends CoGroupFunction<Tuple3<Integer,Integer,Double>,Tuple3<Integer,Integer,double[]>,Tuple2<Integer,double[]>> {

  private int k;
  private double lambda;
  private int id_;
  private Tuple2<Integer,double[]> result_ = new Tuple2(); 

  public PIteration(int k, double lambda) {
    this.k = k; 
    this.lambda = lambda; 
  }
  
  @Override
  public void coGroup(Iterator<Tuple3<Integer,Integer,Double>> matrixElements, Iterator<Tuple3<Integer,Integer,double[]>> q,
      Collector<Tuple2<Integer,double[]>> out) {
    double[][] matrix = new double[k][k];
    double[][] vector = new double[k][1];

    if(!q.hasNext()){
      return;
    }
    
    double element_ = lambda; //Regularization with Frobenius-norm 
     
    if(lambda != 0.0) {
      for(double[] row : matrix) {
        Arrays.fill(row, element_);
      }
    }

    Map<Integer, Double> ratings = new HashMap<Integer, Double>();
    while (matrixElements.hasNext()) {
      //TODO: Fill the ratings map with (columnId,elementValue) pairs from matrixElements
      Tuple3<Integer,Integer,Double> element = matrixElements.next();
      id_ = element.f0;
      ratings.put(element.f1,element.f2);
    }

    while (q.hasNext()) {
      Tuple3<Integer,Integer,double[]> column = q.next();
      double[] column_elements = column.f2;
      id_ = column.f0;
      double rating = ratings.get(column.f1);
      for (int i = 0; i < k; ++i) {
        for (int j = 0; j < k; ++j) {
          matrix[i][j] += column_elements[i] * column_elements[j];
        }
        vector[i][0] += rating * column_elements[i];
      }
    }

    Matrix a = new Matrix(matrix);
    Matrix b = new Matrix(vector);
    Matrix result = a.solve(b);
    
    double[] result_elements = new double[k];
    for (int i = 0; i < k; ++i) {
      result_elements[i] = result.get(i, 0);
    }
    result_.setFields(id_, result_elements);
    out.collect(result_);
  }
 
}
