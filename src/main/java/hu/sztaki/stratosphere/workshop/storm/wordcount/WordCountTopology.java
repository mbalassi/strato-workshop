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

package hu.sztaki.stratosphere.workshop.storm.wordcount;

import eu.stratosphere.streaming.util.PerformanceCounter;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordCountTopology {
	static final String counterPath = "/home/storm/storm-dist/logs/counter/";// "/home/strato/stratosphere-distrib/log/counter/";

	public static class HamletSpout extends BaseRichSpout {
		private static final long serialVersionUID = 1L;

		private PerformanceCounter performanceCounter;
		SpoutOutputCollector _collector;

		final static String path = "/home/storm/storm-dist/resources/hamlet.txt";
		BufferedReader br = null;
		private String line = new String();
		private Values outRecord = new Values("");

		@Override
		public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
			_collector = collector;
			this.performanceCounter = new PerformanceCounter("pc", 1000, 1000, 30000, counterPath + "Spout"
					+ context.getThisTaskId());
		}

		@Override
		public void nextTuple() {
			try {
				br = new BufferedReader(new FileReader(path));
				line = br.readLine().replaceAll("[\\-\\+\\.\\^:,]", "");
				while (line != null) {
					if (line != "") {
						for (String word : line.split(" ")) {
							outRecord.set(0, word);
							_collector.emit(outRecord);
							performanceCounter.count();
						}
					}
					line = br.readLine();
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void ack(Object id) {
		}

		@Override
		public void fail(Object id) {
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));
		}
	}

	public static class WordCount extends BaseRichBolt {
		private static final long serialVersionUID = 1L;

		private PerformanceCounter performanceCounter;
		OutputCollector _collector;

		private Map<String, Integer> wordCounts = new HashMap<String, Integer>();
		private String word = "";
		private Integer count = 0;

		private Values outRecord = new Values("", 0);

		@Override
		public void prepare(Map map, TopologyContext context, OutputCollector collector) {
			_collector = collector;
			this.performanceCounter = new PerformanceCounter("pc", 1000, 1000, 30000, counterPath + "Counter"
					+ context.getThisTaskId());
		}

		@Override
		public void execute(Tuple tuple) {
			word = tuple.getString(0);

			if (wordCounts.containsKey(word)) {
				count = wordCounts.get(word) + 1;
				wordCounts.put(word, count);
			} else {
				count = 1;
				wordCounts.put(word, 1);
			}

			outRecord.set(0, word);
			outRecord.set(1, count);

			_collector.emit(outRecord);
			performanceCounter.count();
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word", "count"));
		}
	}

	public static class Sink extends BaseRichBolt {
		private static final long serialVersionUID = 1L;
		
		@Override
		public void prepare(Map map, TopologyContext context, OutputCollector collector) {
		}

		@Override
		public void execute(Tuple tuple) {
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
		}
	}
	
	public static void main(String[] args) throws Exception {
		

		if (args != null && args.length == 4) {
			int spoutParallelism = Integer.parseInt(args[0]);
			int counterParallelism = Integer.parseInt(args[1]);
			int sinkParallelism = Integer.parseInt(args[2]);
			int numberOfWorkers = Integer.parseInt(args[3]);


			TopologyBuilder builder = new TopologyBuilder();
			builder.setSpout("spout", new HamletSpout(), spoutParallelism);
			builder.setBolt("count", new WordCount(), counterParallelism).fieldsGrouping("spout", new Fields("word"));
			builder.setBolt("sink", new Sink(), sinkParallelism).shuffleGrouping("count");


			Config conf = new Config();
			conf.setDebug(false);
			conf.setNumWorkers(numberOfWorkers);
			
			StormSubmitter.submitTopology("wordcount", conf, builder.createTopology());
		} else {
			System.out.println("USAGE:\n run <spoutParallelism> <counterParallelism> <numberOfWorkers>");
			/*conf.setMaxTaskParallelism(3);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("word-count", conf, builder.createTopology());

			Thread.sleep(50000);

			cluster.shutdown();*/
		}
	}
}
