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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Level;

import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.streaming.api.JobGraphBuilder;
import eu.stratosphere.streaming.api.invokable.UserSinkInvokable;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.streaming.faulttolerance.FaultToleranceType;
import eu.stratosphere.streaming.rabbitmq.RMQSource;
import eu.stratosphere.streaming.util.ClusterUtil;
import eu.stratosphere.streaming.util.LogUtils;

public class ALSPredictionTopology {

	static class GetUserVectorTask extends UserTaskInvokable {
		private static final long serialVersionUID = 1L;

		StreamRecord outputRecord = new StreamRecord(new Tuple2<Long, Double[]>());

		@Override
		public void invoke(StreamRecord record) throws Exception {
			String[] uidVector = record.getString(0).split("#");
			long uid = Long.parseLong(uidVector[0]);
			String[] vectorString = uidVector[1].split(",");

			Double[] vector = new Double[vectorString.length];

			//TODO fill vector 
			
			//TODO fill & emit outputRecord
		}
	}

	public static class PartialTopItemsTask extends UserTaskInvokable {
		private static final long serialVersionUID = 1L;

		StreamRecord outputRecord = new StreamRecord(new Tuple3<Long, Long[], Double[]>());

		// TODO: generate partial item feature matrix from the database
		double[][] partialItemFeature = new double[][] { { 0.1, 0.2, 1 }, { 0.3, 0.4, 1 },
				{ 1., 1., 1 } };
		private int topItemCount;

		Long[] itemIDs = new Long[] { 1L, 10L, 11L }; // Global IDs of the Item
														// partition

		Double[] partialTopItemScores = new Double[topItemCount];
		Long[] partialTopItemIDs = new Long[topItemCount];
		public PartialTopItemsTask(int topItemCount) {
			this.topItemCount = topItemCount;
		}

		@Override
		public void invoke(StreamRecord record) throws Exception {
			Double[] userVector = (Double[]) record.getField(1);
			Double[] scores = new Double[itemIDs.length];

			// calculate scores for all items
			for (int item = 0; item < itemIDs.length; item++) {
				
				//TODO calculate scores for all items

			}

			// get the top TOP_ITEM_COUNT items for the partitions
			partialTopItemIDs = Arrays.copyOfRange(itemIDs, 0, topItemCount);
			partialTopItemScores = Arrays.copyOfRange(scores, 0, topItemCount);

			//TODO fill partialTopItemIDs and partialTopItemScores 

			outputRecord.setField(0, record.getField(0));
			outputRecord.setField(1, partialTopItemIDs);
			outputRecord.setField(2, partialTopItemScores);
			emit(outputRecord);
		}

	}

	public static class TopItemsTask extends UserTaskInvokable {
		private static final long serialVersionUID = 1L;

		private int numberOfPartitions;
		Map<Long, Integer> partitionCount = new HashMap<Long, Integer>();
		Map<Long, Long[]> topIDs = new HashMap<Long, Long[]>();
		Map<Long, Double[]> topScores = new HashMap<Long, Double[]>();

		StreamRecord outputRecord = new StreamRecord(new Tuple3<Long, Long[], Double[]>());

		public TopItemsTask(int numberOfPartitions) {
			this.numberOfPartitions = numberOfPartitions;
		}

		@Override
		public void invoke(StreamRecord record) throws Exception {
			Long uid = record.getLong(0);
			Long[] pTopIds = (Long[]) record.getField(1);
			Double[] pTopScores = (Double[]) record.getField(2);

			if (partitionCount.containsKey(uid)) {

				updateTopItems(uid, pTopIds, pTopScores);
				Integer newCount = partitionCount.get(uid) - 1;

				if (newCount > 0) {
					partitionCount.put(uid, newCount);
				} else {
					outputRecord.setField(0, uid);
					outputRecord.setField(1, topIDs.get(uid));
					outputRecord.setField(2, topScores.get(uid));
					emit(outputRecord);

					partitionCount.remove(uid);
					topIDs.remove(uid);
					topScores.remove(uid);
				}
			} else {
				if (numberOfPartitions == 1) {
					outputRecord.setField(0, uid);
					outputRecord.setField(1, pTopIds);
					outputRecord.setField(2, pTopScores);
					emit(outputRecord);
				} else {
					partitionCount.put(uid, numberOfPartitions - 1);
					topIDs.put(uid, pTopIds);
					topScores.put(uid, pTopScores);
				}
			}
		}

		private void updateTopItems(Long uid, Long[] pTopIDs, Double[] pTopScores) {
			Double[] currentTopScores = topScores.get(uid);
			Long[] currentTopIDs = topIDs.get(uid);

			for (int i = 0; i < pTopScores.length; i++) {
				if (!Arrays.asList(currentTopIDs).contains(pTopIDs[i])) {
					for (int j = 0; j < currentTopScores.length; j++) {
						
						//TODO update top items if needed
					
					}
				}
			}
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
		
		JobGraphBuilder graphBuilder = new JobGraphBuilder("ALS prediction",
				FaultToleranceType.NONE);
		
		graphBuilder.setSource("IDsource", new RMQSource("localhost", "id-queue"), 1, 1);
		graphBuilder.setTask("GetUserVectorTask", new GetUserVectorTask(), 1, 1);
		graphBuilder.setTask("PartialTopItemsTask", new PartialTopItemsTask(topItemCount), partitionCount, partitionCount);
		graphBuilder.setTask("TopItemsTask", new TopItemsTask(partitionCount), 1, 1);
		graphBuilder.setSink("TopItemsProcessorSink", new TopItemsProcessorSink(), 1, 1);

		graphBuilder.shuffleConnect("IDsource", "GetUserVectorTask");
		graphBuilder.broadcastConnect("GetUserVectorTask", "PartialTopItemsTask");
		graphBuilder.fieldsConnect("PartialTopItemsTask", "TopItemsTask", 0);
		graphBuilder.shuffleConnect("TopItemsTask", "TopItemsProcessorSink");

		return graphBuilder.getJobGraph();
	}

	public static void main(String[] arg) {
		LogUtils.initializeDefaultConsoleLogger(Level.DEBUG, Level.INFO);

		ClusterUtil.runOnMiniCluster(getJobGraph(2,2));
	}

}
