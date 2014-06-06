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

import eu.stratosphere.api.java.tuple.Tuple2;

public class MatrixLine extends Tuple2<Integer, double[]> {
	public MatrixLine(){
		super();
	}

	public MatrixLine(int index, double[] values){
		super(index, values);
	}

	public int getIndex() { return f0; }

	public double[] getValues() { return f1; }
}
