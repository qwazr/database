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
package com.qwazr.database.store;

import com.qwazr.utils.LockUtils;
import org.apache.commons.collections4.trie.PatriciaTrie;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

public class UniqueKey<T> {

	private static final Logger logger = LoggerFactory
			.getLogger(UniqueKey.class);

	private Integer higherId;

	private final RoaringBitmap deletedSet;

	private final StoreMapInterface<String, RoaringBitmap> storedKeyDeletedSetMap;

	private final StoreMapInterface<String, Integer> storedKeyHigherMap;

	private final StoreMapInterface<T, Integer> storedUniqueKeyTermMap;

	private final LockUtils.ReadWriteLock rwl = new LockUtils.ReadWriteLock();

	private final PatriciaTrie<Integer> memoryUniqueKeyTermMap;

	private final String keyName;

	UniqueKey(Table table, String keyName, StoreMapInterface<T, Integer> storedUniqueKeyTermMap,
			  PatriciaTrie<Integer> memoryUniqueKeyTermMap)
			throws IOException {

		if (logger.isInfoEnabled())
			logger.info("Create UniqueKey " + keyName);

		this.keyName = keyName;

		this.storedKeyHigherMap = table.storedKeyHigherMap;
		this.storedKeyDeletedSetMap = table.storedKeyDeletedSetMap;
		RoaringBitmap ds = storedKeyDeletedSetMap.get(keyName);
		this.deletedSet = ds == null ? new RoaringBitmap() : ds;
		this.storedUniqueKeyTermMap = storedUniqueKeyTermMap;
		this.memoryUniqueKeyTermMap = memoryUniqueKeyTermMap;
		this.higherId = storedKeyHigherMap.get(keyName);
		if (this.higherId == null)
			this.higherId = 0;
	}

	void deleteKey(T key) throws IOException {
		String keyString = key.toString();
		rwl.r.lock();
		try {
			Integer id = memoryUniqueKeyTermMap.get(keyString);
			if (id == null)
				id = storedUniqueKeyTermMap.get(key);
			if (id == null)
				return;
		} finally {
			rwl.r.unlock();
		}
		rwl.w.lock();
		try {
			Integer id = memoryUniqueKeyTermMap.get(keyString);
			if (id == null)
				id = storedUniqueKeyTermMap.get(key);
			if (id == null)
				return;
			memoryUniqueKeyTermMap.remove(keyString);
			storedUniqueKeyTermMap.remove(key);
			deletedSet.add(id);
			saveDeletedSet();
		} finally {
			rwl.w.unlock();
		}
	}

	private void saveDeletedSet() throws IOException {
		storedKeyDeletedSetMap.put(keyName, deletedSet);
	}

	private void saveHigher() throws IOException {
		storedKeyHigherMap.put(keyName, higherId);
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
		int count = higherId + 1 - deletedSet.getCardinality();
		return count;
	}

	private Integer getExistingIdNoLock(T key) throws IOException {
		String keyString = key.toString();
		Integer id = memoryUniqueKeyTermMap.get(keyString);
		if (id != null)
			return id;
		id = storedUniqueKeyTermMap.get(key);
		if (id != null)
			memoryUniqueKeyTermMap.put(keyString, id);
		return id;
	}

	public Integer getExistingId(T key) throws IOException {
		rwl.r.lock();
		try {
			return getExistingIdNoLock(key);
		} finally {
			rwl.r.unlock();
		}
	}

	void fillExistingIds(Collection<T> keys, Collection<Integer> ids) throws IOException {
		rwl.r.lock();
		try {
			for (T key : keys) {
				Integer id = getExistingIdNoLock(key);
				if (id != null)
					ids.add(id);
			}
		} finally {
			rwl.r.unlock();
		}
	}

	public Integer getIdOrNew(T key, AtomicBoolean isNew) throws IOException {
		if (isNew == null)
			isNew = new AtomicBoolean();
		isNew.set(false);
		rwl.r.lock();
		try {
			Integer id = getExistingIdNoLock(key);
			if (id != null)
				return id;
		} finally {
			rwl.r.unlock();
		}
		rwl.w.lock();
		try {
			Integer id = getExistingIdNoLock(key);
			if (id != null)
				return id;

			// Retrieve an already deleted id
			if (deletedSet != null && !deletedSet.isEmpty()) {
				id = deletedSet.iterator().next();
				storedUniqueKeyTermMap.put(key, id);
				memoryUniqueKeyTermMap.put(key.toString(), id);
				deletedSet.remove(id);
				isNew.set(true);
				saveDeletedSet();
				return id;
			}

			id = higherId + 1;
			storedUniqueKeyTermMap.put(key, id);
			memoryUniqueKeyTermMap.put(key.toString(), id);
			higherId = id;
			isNew.set(true);
			saveDeletedSet();
			saveHigher();
			return id;

		} finally {
			rwl.w.unlock();
		}
	}

	static UniqueKey<String> newPrimaryKey(Table table, String keyName) throws IOException {
		return new UniqueKey<String>(table, keyName, table.storedPrimaryKeyMap, table.memoryPrimaryKeyTermMap);
	}

	static UniqueKey<String> newStringKey(Table table, String keyName) throws IOException {
		return new UniqueKey<String>(table, keyName, table.storedStringKeyMap, table.memoryStringKeyTermMap);
	}

	static UniqueKey<Long> newLongKey(Table table, String keyName) throws IOException {
		return new UniqueKey<Long>(table, keyName, table.storedLongKeyMap, table.memoryLongKeyTermMap);
	}

	static UniqueKey<Double> newDoubleKey(Table table, String keyName) throws IOException {
		return new UniqueKey<Double>(table, keyName, table.storedDoubleKeyMap, table.memoryDoubleKeyTermMap);
	}

}
