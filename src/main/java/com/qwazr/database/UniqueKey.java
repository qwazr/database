/**
 * Copyright 2015 Emmanuel Keller / QWAZR
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qwazr.database;

import com.qwazr.utils.LockUtils;
import com.qwazr.utils.SerializationUtils;
import org.apache.commons.collections4.trie.PatriciaTrie;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class UniqueKey<T> {

	private static final Logger logger = LoggerFactory
			.getLogger(UniqueKey.class);

	private Integer higherId;

	private final RoaringBitmap deletedSet;

	private final File higherIdTempFile;

	private final File higherIdFile;

	private final File trieTempFile;

	private final File trieFile;

	private final File deletedTempFile;

	private final File deletedFile;

	private final Map<T, Integer> keyMap;

	private final LockUtils.ReadWriteLock rwl = new LockUtils.ReadWriteLock();

	private boolean mustBeSaved;

	private final String namePrefix;

	UniqueKey(File directory, String namePrefix) throws IOException {

		if (logger.isInfoEnabled())
			logger.info("Create UniqueKey " + namePrefix);

		this.namePrefix = namePrefix;

		mustBeSaved = false;

		// Load trie
		trieFile = new File(directory, namePrefix + ".trie");
		if (trieFile.exists() && trieFile.length() > 0)
			keyMap = loadKeyMap(trieFile);
		else
			keyMap = getNewKeyMap();
		trieTempFile = new File(directory, "." + trieFile.getName());

		// Load delete ids
		deletedFile = new File(directory, namePrefix + ".deleted");
		if (deletedFile.exists() && deletedFile.length() > 0)
			deletedSet = SerializationUtils.deserialize(deletedFile);
		else
			deletedSet = new RoaringBitmap();
		deletedTempFile = new File(directory, "." + deletedFile.getName());

		// Load the next key
		higherIdFile = new File(directory, namePrefix + ".high");
		if (higherIdFile.exists() && higherIdFile.length() > 0)
			higherId = SerializationUtils.deserialize(higherIdFile);
		else
			higherId = null;
		higherIdTempFile = new File(directory, "." + higherIdFile.getName());

	}

	protected abstract Map<T, Integer> getNewKeyMap();

	protected abstract Map<T, Integer> loadKeyMap(File file)
			throws IOException;

	protected abstract void saveKeyMap(File file) throws IOException;

	void commit() throws IOException {
		rwl.r.lock();
		try {
			if (!mustBeSaved)
				return;
			if (logger.isInfoEnabled())
				logger.info("Commit saved " + namePrefix);
			saveKeyMap(trieTempFile);
			SerializationUtils.serialize(deletedSet, deletedTempFile);
			if (higherId != null)
				SerializationUtils.serialize(higherId, higherIdTempFile);
			trieTempFile.renameTo(trieFile);
			deletedTempFile.renameTo(deletedFile);
			higherIdTempFile.renameTo(higherIdFile);
			mustBeSaved = false;
		} finally {
			rwl.r.unlock();
		}
	}

	void deleteKey(T key) {
		rwl.r.lock();
		try {
			Integer id = keyMap.get(key);
			if (id == null)
				return;
		} finally {
			rwl.r.unlock();
		}
		rwl.w.lock();
		try {
			Integer id = keyMap.remove(key);
			if (id == null)
				return;
			deletedSet.add(id);
			mustBeSaved = true;
		} finally {
			rwl.w.unlock();
		}
	}

	void removeDeleted(RoaringBitmap finalBitmap) {
		rwl.r.lock();
		try {
			if (deletedSet.isEmpty())
				return;
			finalBitmap.andNot(deletedSet);
		} finally {
			rwl.r.unlock();
		}
	}

	public RoaringBitmap getActiveSet() {
		rwl.r.lock();
		try {
			RoaringBitmap bitmap = deletedSet.clone();
			bitmap.flip(0, higherId + 1);
			return bitmap;
		} finally {
			rwl.r.unlock();
		}
	}

	int size() {
		int count1 = keyMap.size();
		int count2 = higherId + 1 - deletedSet.getCardinality();
		if (count1 != count2)
			logger.warn("Size count issue: " + count1 + "/" + count2);
		return count1;
	}

	public Integer getExistingId(T key) {
		rwl.r.lock();
		try {
			return keyMap.get(key);
		} finally {
			rwl.r.unlock();
		}
	}

	void fillExistingIds(Collection<T> keys, Collection<Integer> ids) {
		rwl.r.lock();
		try {
			for (T key : keys)
				ids.add(keyMap.get(key));
		} finally {
			rwl.r.unlock();
		}
	}

	public Integer getIdOrNew(T key, AtomicBoolean isNew) {
		if (isNew == null)
			isNew = new AtomicBoolean();
		isNew.set(false);
		rwl.r.lock();
		try {
			Integer id = keyMap.get(key);
			if (id != null)
				return id;
		} finally {
			rwl.r.unlock();
		}
		rwl.w.lock();
		try {
			Integer id = keyMap.get(key);
			if (id != null)
				return id;

			// Retrieve an already deleted id
			if (!deletedSet.isEmpty()) {
				id = deletedSet.iterator().next();
				keyMap.put(key, id);
				deletedSet.remove(id);
				isNew.set(true);
				mustBeSaved = true;
				return id;
			}

			// Use the next id
			if (higherId == null)
				higherId = findHigher();
			id = higherId + 1;
			keyMap.put(key, id);
			higherId = id;
			isNew.set(true);
			mustBeSaved = true;
			return id;

		} finally {
			rwl.w.unlock();
		}
	}

	private Integer findHigher() {
		Integer found = -1;
		for (Integer id : keyMap.values())
			if (id > found)
				found = id;
		if (logger.isInfoEnabled())
			logger.info("Find higher (" + namePrefix + ") : " + found);
		return found;
	}

	public static class UniqueStringKey extends UniqueKey<String> {

		private PatriciaTrie<Integer> map;

		public UniqueStringKey(File directory, String namePrefix)
				throws IOException {
			super(directory, namePrefix);
		}

		@Override
		protected Map<String, Integer> getNewKeyMap() {
			map = new PatriciaTrie<Integer>();
			return map;
		}

		@Override
		protected Map<String, Integer> loadKeyMap(File file)
				throws IOException {
			map = SerializationUtils.deserialize(file);
			return map;
		}

		@Override
		protected void saveKeyMap(File file) throws IOException {
			SerializationUtils.serialize(map, file);
		}

	}

	public static class UniqueDoubleKey extends UniqueKey<Double> {

		private HashMap<Double, Integer> map;

		public UniqueDoubleKey(File directory, String namePrefix)
				throws IOException {
			super(directory, namePrefix);
		}

		@Override
		protected Map<Double, Integer> getNewKeyMap() {
			map = new HashMap<Double, Integer>();
			return map;
		}

		@Override
		protected Map<Double, Integer> loadKeyMap(File file)
				throws IOException {
			map = SerializationUtils.deserialize(file);
			return map;
		}

		@Override
		protected void saveKeyMap(File file) throws IOException {
			SerializationUtils.serialize(map, file);
		}

	}

	public static class UniqueLongKey extends UniqueKey<Long> {

		private HashMap<Long, Integer> map;

		public UniqueLongKey(File directory, String namePrefix)
				throws IOException {
			super(directory, namePrefix);
		}

		@Override
		protected Map<Long, Integer> getNewKeyMap() {
			map = new HashMap<Long, Integer>();
			return map;
		}

		@Override
		protected Map<Long, Integer> loadKeyMap(File file)
				throws IOException {
			map = SerializationUtils.deserialize(file);
			return map;
		}

		@Override
		protected void saveKeyMap(File file) throws IOException {
			SerializationUtils.serialize(map, file);
		}

	}
}
