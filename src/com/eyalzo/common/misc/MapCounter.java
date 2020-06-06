/**
 * Copyright 2011 Eyal Zohar. All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
 * following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 * disclaimer.
 * 
 * 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY EYAL ZOHAR ''AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * EYAL ZOHAR OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * 
 * The views and conclusions contained in the software and documentation are those of the authors and should not be
 * interpreted as representing official policies, either expressed or implied, of Eyal Zohar.
 */
package com.eyalzo.common.misc;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;

/**
 * Counter of items per key. Use it to count instances in list or map.
 * 
 * @author Eyal Zohar
 */
public class MapCounter<K> {
	protected Map<K, Long> mapCounter = Collections.synchronizedMap(new HashMap<K, Long>());

	/**
	 * Add to counter of this key.
	 * 
	 * @param key
	 *            Key.
	 * @param toAdd
	 *            How much to add to this key's counter.
	 * @return Updated count, after add.
	 */
	public synchronized long add(K key, long toAdd) {
		Long prevValue = mapCounter.get(key);

		//
		// If key is new put for the first time
		//
		if (prevValue == null) {
			mapCounter.put(key, toAdd);
			return toAdd;
		}

		//
		// Key already exists, to add to current
		//
		long newValue = prevValue + toAdd;
		mapCounter.put(key, newValue);
		return newValue;
	}

	/**
	 * Add to counter of these keys.
	 * 
	 * @param keys
	 *            Collection of keys to be handled with iterator.
	 * @param toAdd
	 *            How much to add to keys' counter.
	 */
	public synchronized void addAll(Collection<K> keys, long toAdd) {
		Iterator<K> it = keys.iterator();
		while (it.hasNext()) {
			K key = it.next();
			this.add(key, toAdd);
		}
	}

	/**
	 * Add counters from another map-counter.
	 * 
	 * @param other
	 *            Another map-counter. map-counter, so a local copy better be given and not a live object.
	 */
	public synchronized void addAll(MapCounter<K> other) {
		Iterator<Entry<K, Long>> it = other.entrySet().iterator();
		while (it.hasNext()) {
			Entry<K, Long> entry = it.next();
			this.add(entry.getKey(), entry.getValue());
		}
	}

	/**
	 * Add 1 to counter of this key.
	 * 
	 * @param key
	 *            Key.
	 * @return Updated count, after add.
	 */
	public synchronized long inc(K key) {
		return this.add(key, 1L);
	}

	/**
	 * Sub 1 from counter of this key.
	 * 
	 * @param key
	 *            Key.
	 * @return Updated count, after sub.
	 */
	public synchronized long dec(K key) {
		return this.add(key, -1L);
	}

	/**
	 * Add 1 to counter of these keys.
	 * 
	 * @param keys
	 *            Collection of keys to be handled with iterator.
	 */
	public synchronized void incAll(Collection<K> keys) {
		this.addAll(keys, 1L);
	}

	/**
	 * @return The internal map with counter for every key. Must be synchronized for safe access.
	 */
	public synchronized Map<K, Long> getMap() {
		return mapCounter;
	}

	/**
	 * @return A duplicate of the internal map with counter for every key. No need to synchronize.
	 */
	public synchronized Map<K, Long> getMapDup() {
		return new HashMap<K, Long>(mapCounter);
	}

	/**
	 * @return Entry set, for iterator over the original internal map.
	 */
	public synchronized Set<Entry<K, Long>> entrySet() {
		return mapCounter.entrySet();
	}

	/**
	 * @return Key set, for iterator over the original internal map.
	 */
	public synchronized Set<K> keySet() {
		return mapCounter.keySet();
	}

	/**
	 * Gets the numeric value stored for this key, or zero if not found.
	 * 
	 * @param key
	 *            The given key.
	 * @return The numeric value stored for this key, or zero if not found.
	 */
	public synchronized long get(K key) {
		Long value = mapCounter.get(key);
		if (value == null)
			return 0;
		return value.longValue();
	}

	/**
	 * @return Sum of al counts.
	 */
	public synchronized long getSum() {
		long sum = 0;

		for (long val : mapCounter.values()) {
			sum += val;
		}

		return sum;
	}

	/**
	 * @return Average count per key, by sum divided by number of keys.
	 */
	public synchronized float getAverage() {
		if (mapCounter.isEmpty())
			return 0;

		return (float) (((double) (this.getSum())) / mapCounter.size());
	}

	/**
	 * @return Number of keys in this map.
	 */
	public int size() {
		return mapCounter.size();
	}

	@Override
	public synchronized String toString() {
		return toString("\n", "\t");
	}

	/**
	 * @param keySeparator
	 *            If not null, keys will be returned, and each line (except for the last) will end with this string.
	 * @param valueSeparator
	 *            If not null, values will be returned, and separated from keys (or other values, if keys are not returned) with this string.
	 */
	public synchronized String toString(String keySeparator, String valueSeparator) {
		return toString(this.mapCounter, keySeparator, valueSeparator, 0);
	}

	/**
	 * @param keySeparator
	 *            If not null, keys will be returned, and each line (except for the last) will end with this string.
	 * @param valueSeparator
	 *            If not null, values will be returned, and separated from keys (or other values, if keys are not returned) with this string.
	 */
	public synchronized String toStringSortByKey(String keySeparator, String valueSeparator) {
		return toString(this.getSortedByKeyDup(), keySeparator, valueSeparator, 0);
	}

	/**
	 * @param keySeparator
	 *            If not null, keys will be returned, and each line (except for the last) will end with this string.
	 * @param valueSeparator
	 *            If not null, values will be returned, and separated from keys (or other values, if keys are not returned) with this string.
	 */
	public synchronized String toStringSortByCount(String keySeparator, String valueSeparator, long sum) {
		return toString(this.getSortedByCountDup(), keySeparator, valueSeparator, sum);
	}

	/**
	 * @param map
	 *            The map to use: can be the internal map or a sorted one.
	 * @param keySeparator
	 *            If not null, keys will be returned, and each line (except for the last) will end with this string.
	 * @param valueSeparator
	 *            If not null, values will be returned, and separated from keys (or other values, if keys are not returned) with this string.
	 */
	private synchronized String toString(Map<K, Long> map, String keySeparator, String valueSeparator, long sum) {
		StringBuffer buffer = new StringBuffer(1000);
		boolean first = true;

		Iterator<Entry<K, Long>> it = map.entrySet().iterator();
		while (it.hasNext()) {
			Entry<K, Long> entry = it.next();

			if (keySeparator != null) {
				if (first) {
					first = false;
				} else {
					buffer.append(keySeparator);
				}
				buffer.append(entry.getKey() == null ? "(null)" : entry.getKey().toString());
			}

			// Value
			if (valueSeparator != null) {
				buffer.append(valueSeparator);

				// Value itself
				long value = entry.getValue();
				buffer.append(String.format("%,d", value));

				if (sum > 0) {
					buffer.append(valueSeparator);
					// Percents
					buffer.append(String.format("%.3f%%", 100.0 * value / sum));
				}
			}
		}

		return buffer.toString();
	}

	/**
	 * Clear all items and their counters.
	 */
	public synchronized void clear() {
		mapCounter.clear();
	}

	/**
	 * @param minValue
	 *            Keys with value below this will be removed.
	 */
	public synchronized void cleanupMin(long minValue) {
		Iterator<Long> it = mapCounter.values().iterator();
		while (it.hasNext()) {
			Long curValue = it.next();
			if (curValue < minValue) {
				it.remove();
			}
		}
	}

	/**
	 * @param factor
	 *            All the values will be divided by this number.
	 */
	public synchronized void div(long factor) {
		Iterator<Entry<K, Long>> it = mapCounter.entrySet().iterator();
		while (it.hasNext()) {
			Entry<K, Long> entry = it.next();

			entry.setValue(entry.getValue() / factor);
		}
	}

	/**
	 * @param factor
	 *            All the values will be multiplied by this number.
	 */
	public synchronized void mul(double factor) {
		Iterator<Entry<K, Long>> it = mapCounter.entrySet().iterator();
		while (it.hasNext()) {
			Entry<K, Long> entry = it.next();

			entry.setValue((long) (entry.getValue() * factor));
		}
	}

	public synchronized boolean isEmpty() {
		return mapCounter.isEmpty();
	}

	/**
	 * @return The key with the highest count attached to it, or null if empty.
	 */
	public synchronized K getMaxKey() {
		K result = null;
		long max = Long.MIN_VALUE;

		Iterator<Entry<K, Long>> it = mapCounter.entrySet().iterator();
		while (it.hasNext()) {
			Entry<K, Long> entry = it.next();

			if (entry.getValue() > max) {
				result = entry.getKey();
				max = entry.getValue();
			}
		}

		return result;
	}

	/**
	 * @return The highest count or 0 if empty.
	 */
	public synchronized long getMaxCount() {
		K maxKey = getMaxKey();
		if (maxKey == null)
			return 0;

		return mapCounter.get(maxKey);
	}

	/**
	 * @return The key with the lowest count attached to it, or null if empty.
	 */
	public synchronized K getMinKey() {
		K result = null;
		long min = Long.MAX_VALUE;

		Iterator<Entry<K, Long>> it = mapCounter.entrySet().iterator();
		while (it.hasNext()) {
			Entry<K, Long> entry = it.next();

			if (entry.getValue() < min) {
				result = entry.getKey();
				min = entry.getValue();
			}
		}

		return result;
	}

	/**
	 * @return New map, sorted by the keys, in ascending order.
	 */
	public synchronized SortedMap<K, Long> getSortedByKeyDup() {
		return new TreeMap<K, Long>(mapCounter);
	}

	/**
	 * Use with care!!! the sorted map does not follow the equals() rules, and in case different keys return the same toString() the keys may merge.
	 * 
	 * @return New map, sorted by the counters, in ascending order. In case of equality, order is random. Note: can be easily scanned in reverse order by using
	 *         {@link TreeMap#descendingMap()}.
	 */
	public synchronized TreeMap<K, Long> getSortedByCountDup() {
		TreeMap<K, Long> result = new TreeMap<K, Long>(new Comparator<K>() {
			public int compare(K o1, K o2) {
				Long val1 = mapCounter.get(o1);
				Long val2 = mapCounter.get(o2);
				int result = val1.compareTo(val2);
				// Very important - when counters are equal, return order by
				// key, because otherwise items will be
				// considered identical
				if (result == 0)
					result = (o1.toString()).compareTo(o2.toString());
				return result;
			}
		});

		result.putAll(mapCounter);

		return result;
	}

	/**
	 * Use with care!!! the sorted map does not follow the equals() rules, and in case different keys return the same toString() the keys may merge.
	 * 
	 * @return New map, sorted by the counters, in ascending order. In case of equality, order is random. Note: can be easily scanned in reverse order by using
	 *         {@link TreeMap#descendingMap()}.
	 */
	public synchronized TreeMap<K, Long> getSortedByCountDup(long minCount) {
		TreeMap<K, Long> result = new TreeMap<K, Long>(new Comparator<K>() {
			public int compare(K o1, K o2) {
				Long val1 = mapCounter.get(o1);
				Long val2 = mapCounter.get(o2);
				int result = val1.compareTo(val2);
				// Very important - when counters are equal, return order by
				// key, because otherwise items will be
				// considered identical
				if (result == 0)
					result = (o1.toString()).compareTo(o2.toString());
				return result;
			}
		});

		// Add only items with minimal given count
		for (Entry<K, Long> entry : mapCounter.entrySet()) {
			long value = entry.getValue();
			if (value >= minCount)
				result.put(entry.getKey(), value);
		}

		return result;
	}

	/**
	 * @param keyToClear
	 *            Key to remove from the map.
	 */
	public void clear(K keyToClear) {
		synchronized (mapCounter) {
			mapCounter.remove(keyToClear);
		}
	}

	public boolean containsKey(K searchKey) {
		return this.mapCounter.containsKey(searchKey);
	}

	public MapCounter<Long> getMapCounterOfCounts() {
		MapCounter<Long> result = new MapCounter<Long>();

		synchronized (mapCounter) {
			for (Long curCount : mapCounter.values()) {
				result.inc(curCount);
			}
		}

		return result;
	}

	public void filterMinMax(long minCount, long maxCount) {
		synchronized (mapCounter) {
			for (Iterator<Long> it = mapCounter.values().iterator(); it.hasNext();) {
				long value = it.next();
				if (value < minCount || value > maxCount)
					it.remove();
			}
		}
	}

	/**
	 * @param repValueMemberCount
	 *            The count-value to search for.
	 * @return Set of keys that have the exact given count-value. May be emtpy, but never null.
	 */
	public HashSet<K> getKeysExactValue(long repValueMemberCount) {
		HashSet<K> result = new HashSet<K>();

		for (Entry<K, Long> entry : mapCounter.entrySet()) {
			if (entry.getValue() == repValueMemberCount)
				result.add(entry.getKey());
		}

		return result;
	}
}
