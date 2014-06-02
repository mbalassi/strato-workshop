package hu.sztaki.stratosphere.workshop.batch.customals;

import java.util.Collection;
import java.util.Iterator;

import eu.stratosphere.util.Collector;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.tuple.Tuple5;

public class MultiplyMatrix extends MapFunction<Tuple3<Integer,Integer,Double>,Tuple5<Integer,Boolean,Integer,Integer,double[]>> {

  private int numOfTasks;
  private Tuple5<Integer,Boolean,Integer,Integer,double[]> output = new Tuple5();
  private int index;
  private static final boolean TRUE = true;//represents that the output Tuple corresponds to a matrix element
  
  public MultiplyMatrix(int numTask, int index) {
    this.numOfTasks = numTask; 
    this.index = index;
  }
  
  @Override
  public Tuple5<Integer,Boolean,Integer,Integer,double[]> map(Tuple3<Integer,Integer, Double> record) throws Exception {

    int ownIndex = record.getField(index);//when ==0 then we do rowwise partition, when ==1 we do a columnwise partition
    
    //TODO: assign each element of the (sparse) rating matrix uniformly to a machine
    //TODO: the output vector has the (machinceIndex, TRUE, rowID, columnID, double[1]{elementValue}) format
    
    return null;
  
  }

}
