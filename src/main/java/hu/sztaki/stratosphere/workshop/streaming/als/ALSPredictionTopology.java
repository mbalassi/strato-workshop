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

package hu.sztaki.stratosphere.workshop.streaming.als;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Level;

import hu.sztaki.stratosphere.workshop.utils.Util;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.streaming.api.JobGraphBuilder;
import eu.stratosphere.streaming.api.invokable.UserSinkInvokable;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.streaming.rabbitmq.RMQSource;
import eu.stratosphere.streaming.util.ClusterUtil;
import eu.stratosphere.streaming.util.LogUtils;

public class ALSPredictionTopology {

	static class GetUserVectorTask extends UserTaskInvokable {
		private static final long serialVersionUID = 1L;

		// This array contains the user feature vectors
		private Double[][] userVectorMatrix = Util.getUserMatrix();

		StreamRecord outputRecord = new StreamRecord(new Tuple2<Integer, Double[]>());

		@Override
		public void invoke(StreamRecord record) throws Exception {
			String uidString = record.getString(0);

			// TODO parse uid from uidString
			// TODO fill & emit outputRecord (uid, uservector)
		}
	}

	public static class PartialTopItemsTask extends UserTaskInvokable {
		private static final long serialVersionUID = 1L;

		private int topItemCount;

		// array containing item feature vectors
		double[][] partialItemFeature;

		// global IDs of the Item partition
		Integer[] itemIDs;
		private int partitionSize;

		Double[] partialTopItemScores;
		Integer[] partialTopItemIDs;

		// TODO create outputRecord object (uid, partialTopItemIDs,
		// partialTopItemScores)

		public PartialTopItemsTask(int numberOfPartitions, int topItemCount) {
			this.topItemCount = topItemCount;

			partialItemFeature = Util.getItemMatrix(numberOfPartitions);
			itemIDs = Util.getItemIDs();
			partitionSize = itemIDs.length;

			partialTopItemScores = new Double[topItemCount];
			partialTopItemIDs = new Integer[topItemCount];
		}

		@Override
		public void invoke(StreamRecord record) throws Exception {
			Double[] userVector = (Double[]) record.getField(1);
			Double[] scores = new Double[partitionSize];

			// TODO calculate scores for all items
			for (int item = 0; item < partitionSize; item++) {
				// TODO calculate scalar products of the item feature vectors
				// and user vector
			}

			// TODO get the top topItemCount items for the partitions
			// use Util.getTopK() method

			// TODO fill & emit outputRecord
		}

	}

	public static class TopItemsTask extends UserTaskInvokable {
		private static final long serialVersionUID = 1L;

		private int numberOfPartitions;

		// mapping the user ID to the global top items of the user
		// partitionCount counts down to 0 (till the all the partitions are
		// processed)
		Map<Integer, Integer> partitionCount = new HashMap<Integer, Integer>();
		Map<Integer, Integer[]> topIDs = new HashMap<Integer, Integer[]>();
		Map<Integer, Double[]> topScores = new HashMap<Integer, Double[]>();

		StreamRecord outputRecord = new StreamRecord(new Tuple3<Integer, Integer[], Double[]>());

		public TopItemsTask(int numberOfPartitions) {
			this.numberOfPartitions = numberOfPartitions;
		}

		@Override
		public void invoke(StreamRecord record) throws Exception {
			Integer uid = record.getInteger(0);
			Integer[] pTopIds = (Integer[]) record.getField(1);
			Double[] pTopScores = (Double[]) record.getField(2);

			if (partitionCount.containsKey(uid)) {
				// we already have the user in the maps

				updateTopItems(uid, pTopIds, pTopScores);

				Integer newCount = partitionCount.get(uid) - 1;

				// TODO check if we already have the global top
				// (emit and remove from maps or count down)
			} else {
				// the user is not in the maps

				if (numberOfPartitions == 1) {
					// if there's only one partition,
					// that has the global top scores
					outputRecord.setField(0, uid);
					outputRecord.setField(1, pTopIds);
					outputRecord.setField(2, pTopScores);
					emit(outputRecord);
				} else {
					// if there are more partitions the first one is has the
					// initial
					// top scores
					partitionCount.put(uid, numberOfPartitions - 1);
					topIDs.put(uid, pTopIds);
					topScores.put(uid, pTopScores);
				}
			}
		}

		private void updateTopItems(Integer uid, Integer[] pTopIDs, Double[] pTopScores) {
			Double[] currentTopScores = topScores.get(uid);
			Integer[] currentTopIDs = topIDs.get(uid);

			// TODO update top IDs by using Util.merge()
		}
	}

	public static class TopItemsProcessorSink extends UserSinkInvokable {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(StreamRecord record) throws Exception {
			System.out.println(record);
		}

	}

	public static JobGraph getJobGraph(int partitionCount, int topItemCount) {

		JobGraphBuilder graphBuilder = new JobGraphBuilder("ALS prediction");

		graphBuilder.setSource("IDsource", new RMQSource("localhost", "hello"), 1, 1);
		graphBuilder.setTask("GetUserVectorTask", new GetUserVectorTask());

		// TODO set the two remaining tasks and the sink

		// TODO connect the remaining components using the right connection type

		return graphBuilder.getJobGraph();
	}

	public static void main(String[] arg) {
		LogUtils.initializeDefaultConsoleLogger(Level.ERROR, Level.INFO);
		ClusterUtil.runOnMiniCluster(getJobGraph(2, 2));
	}
}
