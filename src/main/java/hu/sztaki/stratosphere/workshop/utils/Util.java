package hu.sztaki.stratosphere.workshop.utils;

import java.util.TreeSet;

public class Util {

	private static class Item implements Comparable<Item> {
		private Long id;
		private Double value;

		public Item(Long id, Double value) {
			this.id = id;
			this.value = value;
		}

		public Long getId() {
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
	public static void getTopK(int k, Long[] topIDs, Double[] topValues, Long[] ids, Double[] values) {
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
	 * Merges two ordered array pairs and puts them into topIDs and topValues arrays
	 * @param topIDs
	 * result ID array
	 * @param topValues
	 * result values array
	 * @param otherIDs
	 * array to merge topIDs with
	 * @param otherValues
	 * array to merge topValues with
	 */
	public static void merge(Long[] topIDs, Double[] topValues, Long[] otherIDs,
			Double[] otherValues) {

		int ind = 0;
		int oneIndex = 0;
		int otherIndex = 0;

		int k = topValues.length;
		Long[] newTopIDs = new Long[k];
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
}
