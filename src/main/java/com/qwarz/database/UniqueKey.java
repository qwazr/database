/**
 * Copyright 2015 OpenSearchServer Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qwarz.database;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.collections4.trie.PatriciaTrie;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qwazr.utils.LockUtils;
import com.qwazr.utils.SerializationUtils;

public class UniqueKey {

	private static final Logger logger = LoggerFactory
			.getLogger(UniqueKey.class);

	private Integer higherId;

	private final RoaringBitmap deletedSet;

	private final File higherIdFile;

	private final File trieFile;

	private final File deletedFile;

	private final PatriciaTrie<Integer> keyTrie;

	private final LockUtils.ReadWriteLock rwl = new LockUtils.ReadWriteLock();

	private boolean mustBeSaved;

	private final String namePrefix;

	public UniqueKey(File directory, String namePrefix)
			throws FileNotFoundException {

		if (logger.isInfoEnabled())
			logger.info("Create UniqueKey " + namePrefix);

		this.namePrefix = namePrefix;

		mustBeSaved = false;

		// Load trie
		trieFile = new File(directory, namePrefix + ".trie");
		if (trieFile.exists() && trieFile.length() > 0)
			keyTrie = SerializationUtils.deserialize(trieFile);
		else
			keyTrie = new PatriciaTrie<Integer>();

		// Load delete ids
		deletedFile = new File(directory, namePrefix + ".deleted");
		if (deletedFile.exists() && deletedFile.length() > 0)
			deletedSet = SerializationUtils.deserialize(deletedFile);
		else
			deletedSet = new RoaringBitmap();

		// Load the next key
		higherIdFile = new File(directory, namePrefix + ".high");
		if (higherIdFile.exists() && higherIdFile.length() > 0)
			higherId = SerializationUtils.deserialize(higherIdFile);
		else
			higherId = null;

	}

	public void commit() throws FileNotFoundException {
		rwl.r.lock();
		try {
			if (!mustBeSaved)
				return;
			if (logger.isInfoEnabled())
				logger.info("Commit saved " + namePrefix);
			SerializationUtils.serialize(keyTrie, trieFile);
			SerializationUtils.serialize(deletedSet, deletedFile);
			if (higherId != null)
				SerializationUtils.serialize(higherId, higherIdFile);
			mustBeSaved = false;
		} finally {
			rwl.r.unlock();
		}
	}

	public void deleteKey(String key) {
		rwl.r.lock();
		try {
			Integer id = keyTrie.get(key);
			if (id == null)
				return;
		} finally {
			rwl.r.unlock();
		}
		rwl.w.lock();
		try {
			Integer id = keyTrie.remove(key);
			if (id == null)
				return;
			deletedSet.add(id);
			mustBeSaved = true;
		} finally {
			rwl.w.unlock();
		}
	}

	Integer getExistingId(String key) {
		rwl.r.lock();
		try {
			return keyTrie.get(key);
		} finally {
			rwl.r.unlock();
		}
	}

	void fillExistingIds(Collection<String> keys, Collection<Integer> ids) {
		rwl.r.lock();
		try {
			for (String key : keys)
				ids.add(keyTrie.get(key));
		} finally {
			rwl.r.unlock();
		}
	}

	public Integer getIdOrNew(String key, AtomicBoolean isNew) {
		if (isNew == null)
			isNew = new AtomicBoolean();
		isNew.set(false);
		rwl.r.lock();
		try {
			Integer id = keyTrie.get(key);
			if (id != null)
				return id;
		} finally {
			rwl.r.unlock();
		}
		rwl.w.lock();
		try {
			Integer id = keyTrie.get(key);
			if (id != null)
				return id;

			// Retrieve an already deleted id
			if (!deletedSet.isEmpty()) {
				id = deletedSet.iterator().next();
				keyTrie.put(key, id);
				deletedSet.remove(id);
				isNew.set(true);
				mustBeSaved = true;
				return id;
			}

			// Use the next id
			if (higherId == null)
				higherId = findHigher();
			id = higherId + 1;
			keyTrie.put(key, id);
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
		for (Integer id : keyTrie.values())
			if (id > found)
				found = id;
		if (logger.isInfoEnabled())
			logger.info("Find higher (" + namePrefix + ") : " + found);
		return found;
	}

}
