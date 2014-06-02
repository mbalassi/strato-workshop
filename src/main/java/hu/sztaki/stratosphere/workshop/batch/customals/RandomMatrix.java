package hu.sztaki.stratosphere.workshop.batch.customals;

import java.util.Iterator;
import java.util.Collection;
import java.util.Random;

import eu.stratosphere.util.Collector;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.tuple.Tuple5;

public class RandomMatrix extends GroupReduceFunction<Tuple3<Integer,Integer,Double>,Tuple5<Integer,Boolean,Integer, Integer, double[]>> {

  private int k;
  private int numOfTasks;

  private double[] vector_elements;
  private Tuple5<Integer, Boolean, Integer, Integer, double[]> vector = new Tuple5();
  private final Random RANDOM = new Random();
  private static final boolean FALSE = false;//represents that these vectors do not represent an element of the rating matrix
  private static final Integer ZERO = 0;
  
  public RandomMatrix(int numTask, int k) {
    this.numOfTasks = numTask; 
    this.k = k;
  }

  @Override
  public void reduce(Iterator<Tuple3<Integer,Integer,Double>> elements, Collector<Tuple5<Integer, Boolean, Integer, Integer, double[]>> out) 
   throws Exception {
    Tuple3<Integer,Integer,Double> element = elements.next();
    vector_elements = new double[k];
    
    //TODO: Generate a k length random vector for each column of Q. Hint: generate the elements from the [1,1.5] interval.
   


    //TODO: Collect the generated column of Q for all machineId. 
    //Hint: use the following format: (machineId,FALSE,columnId,ZERO,random vector of the column) 
  
  
  
  }
}
