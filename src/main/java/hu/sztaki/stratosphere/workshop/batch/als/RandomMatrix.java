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
  
  
    
    //TODO: collect the generated columns in the given Collector.

  
  }
}
