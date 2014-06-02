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
    
    double element_ = lambda; //Regularization with Frobenius-norm 
     
    if(lambda != 0.0) {
      for(double[] row : matrix) {
        Arrays.fill(row, element_);
      }
    }

    Map<Integer, Double> ratings = new HashMap<Integer, Double>();
    while (matrixElements.hasNext()) {
      
      //TODO: Fill the ratings map with (columnId,elementValue) pairs from matrixElements
    
    
    
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
