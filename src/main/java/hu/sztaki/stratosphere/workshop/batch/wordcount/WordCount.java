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

package hu.sztaki.stratosphere.workshop.batch.wordcount;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.util.Collector;

public class WordCount {
	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		String path = WordCount.class.getResource("/testdata/hamlet.txt").getPath();

		DataSet<String> text = env.readTextFile(path);

		DataSet<Tuple2<String, Integer>> wordCounts = text
				.flatMap(new LineSplitter())
				.groupBy(0)
				.aggregate(Aggregations.SUM, 1);

		wordCounts.print();

		env.execute("Word Count Example");
	}

	public static final class LineSplitter extends FlatMapFunction<String, Tuple2<String, Integer>> {
		@Override
		public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
			for (String word : line.split(" ")) {
				out.collect(new Tuple2<String, Integer>(word, 1));
			}
		}
	}
}
