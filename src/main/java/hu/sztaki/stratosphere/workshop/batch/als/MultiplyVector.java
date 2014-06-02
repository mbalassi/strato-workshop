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
import java.util.Iterator;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;

public class MultiplyVector
		extends
		JoinFunction<Tuple3<Integer, Integer, Double>, Tuple2<Integer, double[]>, Tuple3<Integer, Integer, double[]>> {

	@Override
	public Tuple3<Integer, Integer, double[]> join(Tuple3<Integer, Integer, Double> matrixElement,
			Tuple2<Integer, double[]> columnOfQ) {

		// TODO: send the given j^th column of Q matrix with all i rowId where
		// the a_ij element is given in the A rating matrix.

		return null;

	}

}
