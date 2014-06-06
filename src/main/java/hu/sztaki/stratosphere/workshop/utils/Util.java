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
package hu.sztaki.stratosphere.workshop.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

public class Util {

	private static class Item implements Comparable<Item> {
		private Integer id;
		private Double value;

		public Item(Integer id, Double value) {
			this.id = id;
			this.value = value;
		}

		public Integer getId() {
			return id;
		}

		public Double getValue() {
			return value;
		}

		@Override
		public int compareTo(Item other) {
			return (other.value).compareTo(value);
		}
	}

	/**
	 * Sets the topValues array to the largest k elements in values (in sorted
	 * order)
	 * 
	 * @param k
	 *            number of wished top elements
	 * @param topIDs
	 *            item ID array to change
	 * @param topValues
	 *            item score array to change
	 * @param ids
	 *            item ID array to get the IDs of largest values from
	 * @param values
	 *            values array to get the largest values from
	 */
	public static void getTopK(int k, Integer[] topIDs, Double[] topValues, Integer[] ids, Double[] values) {
		if (k > ids.length) {
			k = ids.length;
		}

		TreeSet<Item> topItems = new TreeSet<Item>();

		int i;
		for (i = 0; i < k; i++) {
			topItems.add(new Item(ids[i], values[i]));
		}

		while (i < k) {
			topItems.add(new Item(ids[i], values[i]));
			topItems.pollLast();
			i++;
		}

		i = 0;
		for (Item item : topItems) {
			topIDs[i] = item.getId();
			topValues[i] = item.getValue();
			i++;
		}
	}

	/**
	 * Merges two ordered array pairs and puts them into topIDs and topValues
	 * arrays
	 * 
	 * @param topIDs
	 *            result ID array
	 * @param topValues
	 *            result values array
	 * @param otherIDs
	 *            array to merge topIDs with
	 * @param otherValues
	 *            array to merge topValues with
	 */
	public static void merge(Integer[] topIDs, Double[] topValues, Integer[] otherIDs,
			Double[] otherValues) {

		int ind = 0;
		int oneIndex = 0;
		int otherIndex = 0;

		int k = topValues.length;
		Integer[] newTopIDs = new Integer[k];
		Double[] newTopValues = new Double[k];

		while (ind < k && oneIndex < topValues.length && otherIndex < otherValues.length) {
			if (topValues[oneIndex] < otherValues[otherIndex]) {
				newTopValues[ind] = otherValues[otherIndex];
				newTopIDs[ind++] = otherIDs[otherIndex++];
			} else {
				newTopValues[ind] = topValues[oneIndex];
				newTopIDs[ind++] = topIDs[oneIndex++];
			}
		}

		while (ind < k && otherIndex < otherValues.length) {
			newTopValues[ind] = otherValues[otherIndex];
			newTopIDs[ind++] = otherIDs[otherIndex++];
		}

		while (ind < k && oneIndex < topValues.length) {
			newTopValues[ind] = topValues[oneIndex];
			newTopIDs[ind++] = topIDs[oneIndex++];
		}

		for (int i = 0; i < newTopIDs.length; i++) {
			topIDs[i] = newTopIDs[i];
			topValues[i] = newTopValues[i];
		}
	}

	static int numOfPartitions;
	static int currentPartition = 0;
	static boolean read = false;
	static List<double[][]> itemMatrix = new ArrayList<double[][]>();

	public static double[][] getItemMatrix(int k) {
		numOfPartitions = k;
		if (!read) {
			List<double[]> rows = new ArrayList<double[]>();
			try {
				BufferedReader br = new BufferedReader(new FileReader(
						"src/main/resources/testdata/als_streaming/sampledb2/q"));
				while (true) {
					String line = br.readLine();
					String[] nums;
					if (line == null) {
						break;
					} else
						nums = line.split("\\|");
					double[] row = new double[nums.length - 1];
					for (int i = 1; i <= row.length; i++) {
						row[i - 1] = Double.parseDouble(nums[i]);
					}
					rows.add(row);

				}
				br.close();
			} catch (Exception e) {
				e.printStackTrace();
			}

			for (int i = 0; i < k; i++) {
				double[][] prows = new double[rows.size() / k][];
				for (int j = 0; j < (rows.size() / k); j++) {
					prows[j] = (rows.get((rows.size() / k) * i + j));
				}
				itemMatrix.add(prows);

			}
			// parse files
			read = true;

		}
		double[][] rm = itemMatrix.get(currentPartition);
		currentPartition++;
		return rm;
	}

	public static Double[][] getUserMatrix() {
		List<Double[]> rows = new ArrayList<Double[]>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(
					"src/main/resources/testdata/als_streaming/sampledb2/p"));
			while (true) {
				String line = br.readLine();
				String[] nums;
				if (line == null) {
					break;
				} else
					nums = line.split("\\|");
				Double[] row = new Double[nums.length - 1];
				for (int i = 1; i <= row.length; i++) {
					row[i - 1] = Double.parseDouble(nums[i]);
				}
				rows.add(row);

			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		Double[][] userMatrix = new Double[rows.size()][];
		for (int i = 0; i < rows.size(); i++) {
			userMatrix[i] = rows.get(i);
		}
		return userMatrix;

	}

	public static Integer[] getItemIDs() {
		int len = itemMatrix.get(0).length;
		Integer[] ids = new Integer[len];
		for (int i = 0; i < len; i++) {
			ids[i] = new Integer((currentPartition - 1) * len + i);
		}
		return ids;
	}
}
