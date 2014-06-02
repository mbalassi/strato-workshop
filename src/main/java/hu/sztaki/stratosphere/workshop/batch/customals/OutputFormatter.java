package hu.sztaki.stratosphere.workshop.batch.customals;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import eu.stratosphere.util.Collector;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple5;

public class OutputFormatter extends GroupReduceFunction<Tuple5<Integer,Boolean,Integer,Integer,double[]>,
       Tuple2<Integer,double[]>> {

  private int numOfTasks;
  private Tuple2<Integer,double[]> output = new Tuple2();
  private Set<Integer> ids_ = new HashSet<Integer>(); 
  
  public OutputFormatter(int numTaks) {
    this.numOfTasks = numTaks;
  }
  
  @Override
  public void reduce(Iterator<Tuple5<Integer,Boolean,Integer,Integer,double[]>> records, Collector<Tuple2<Integer,double[]>> out)
      throws Exception {
    
    ids_.clear(); 
    while (records.hasNext()) {
      
      //Delete the first two marker fields of the vectors and send each vector only once
      //(Duplication of the same column do occur with several machince IDs as we sent the results for all machine in Iteration)
      
    
    
    
    
    
    

    
    }
  }

}
