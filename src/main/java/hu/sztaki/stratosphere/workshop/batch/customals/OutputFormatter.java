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
  private MatrixLine output = new MatrixLine();
  private Set<Integer> ids_ = new HashSet<Integer>(); 
  
  public OutputFormatter(int numTaks) {
    this.numOfTasks = numTaks;
  }
  
  @Override
  public void reduce(Iterator<Partition<MatrixLine>> records, Collector<MatrixLine> out)
      throws Exception {

	  ids_.clear();
	  while (records.hasNext()) {
		  Partition<MatrixLine> record = records.next();
		  int index = record.f0;
		  int id = record.f1.f0;
		  if(!ids_.contains(id)){
			  ids_.add(id);
			  if (id % numOfTasks == index) {
				  output.setFields(id,record.f1.f1);
				  out.collect(output);
			  }
		  }
	  }
  }

}
