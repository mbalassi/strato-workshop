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
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import Jama.Matrix;
import eu.stratosphere.util.Collector;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.tuple.Tuple5;

public class Iteration extends GroupReduceFunction<Tuple5<Integer,Boolean,Integer,Integer,double[]>, 
       Tuple5<Integer,Boolean,Integer,Integer,double[]>> {
  
  private int k;
  private int numOfTasks;
  private double lambda;
  
  private int idx;
  private final Tuple5<Integer,Boolean,Integer,Integer,double[]> output = new Tuple5();
  private static final Boolean FALSE = false;
  private static final Integer ZERO = 0;

  public Iteration(int numTasks, int k, double lambda, int index) {
    this.numOfTasks = numTasks; 
    this.k = k;
    this.idx = index;
    this.lambda = lambda; 
  }

  @Override
  public void reduce(Iterator<Tuple5<Integer,Boolean,Integer,Integer,double[]>> records,
		  Collector<Tuple5<Integer,Boolean,Integer,Integer,double[]>> out)throws Exception {
    Map<Integer, List<IntDoublePair>> matrixElements = 
        new HashMap<Integer, List<IntDoublePair>>();
    Map<Integer, double[]> vectors = new HashMap<Integer, double[]>();
    
    while (records.hasNext()) {
      Tuple5<Integer,Boolean,Integer,Integer,double[]> record = records.next();
      boolean isMatrix = record.f1;
      //we sort our data according to whether it corresponds to an element of the rating matrix or a column of P or Q
      if (isMatrix) {
        addToMatrixMap(matrixElements, record);
      } else {
        addToVectorMap(vectors, record);
      }
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
      Map<Integer, List<IntDoublePair>> matrixElements, Map<Integer, double[]> vectors) {

    double[][] matrix = new double[k][k];
    
    double element = lambda; //Lambda-regularization 
    
    if(lambda != 0.0) {
      for(double[] row : matrix) {
        Arrays.fill(row, element);
      }
    }

    double[][] column = new double[k][1];
    List<IntDoublePair> list = matrixElements.get(recordIndex);
    
    for (IntDoublePair pair : list) {
      double rating = pair.value;
      double[] vector = vectors.get(pair.index);
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
    Tuple5<Integer,Boolean,Integer,Integer,double[]> record) {
    //TODO: add the given vector's element to the map
    //Hint: the map contains (columnID, vectorOfTheElements) pairs
  
  }

  private void addToMatrixMap(Map<Integer, List<IntDoublePair>> map, Tuple5<Integer,Boolean,Integer,Integer,double[]> record) throws Exception {
    int ownIndex = record.f0;
    int recordIndex = record.getField(2 + idx);//if idx==0 then we update P given the columns of Q 
    int otherIndex = record.getField(3 - idx);//if idx==1 then we update Q given the rows of P
    double[] elements = record.f4;
    double value = elements[0];
    
    //Hint: each IntDoublePair is a (otherIndex,value) pair. A List of these object corresponds to each vector which marked for update and has the recordIndex identifier.
    //TODO: store the incoming record's fields in the given map, but make sure there is no duplication of the data
  

  }
}
