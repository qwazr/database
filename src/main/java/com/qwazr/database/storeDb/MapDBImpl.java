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
 **/
package com.qwazr.database.storeDb;

import org.mapdb.Atomic;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.File;
import java.util.Map;

public class MapDBImpl implements StoreInterface {

	private final DB db;

	public MapDBImpl(File file) {
		db = DBMaker.newFileDB(file)
				.cacheLRUEnable().closeOnJvmShutdown()
				.compressionEnable().make();
	}

	@Override
	public void close() {
		if (!db.isClosed())
			db.close();
	}

	@Override
	public <K, V> StoreMap<K, V> getMap(String mapName, ByteConverter<K> keyConverter, ByteConverter<V> valueConverter) {
		return new StoreMapImpl(mapName);
	}

	public LongSequence getLongSequence(String sequenceName) {
		return new LongSequenceImpl(sequenceName);
	}

	@Override
	public void commit() {
		db.commit();
	}

	@Override
	public void delete(String collectionName) {
		db.delete(collectionName);
	}

	@Override
	public boolean exists(String collectionName) {
		return db.exists(collectionName);
	}

	@Override
	public void rollback() {
		db.rollback();
	}

	private class LongSequenceImpl implements LongSequence {

		private final Atomic.Long longSequence;

		private LongSequenceImpl(String sequenceName) {
			Atomic.Long ls = db.getAtomicLong(sequenceName);
			if (ls == null)
				ls = db.createAtomicLong(sequenceName, 0);
			longSequence = ls;
		}

		@Override
		final public Long incrementAndGet() {
			return longSequence.incrementAndGet();
		}
	}

	private class StoreMapImpl<K, V> implements StoreMap<K, V> {

		private final Map<K, V> map;

		private StoreMapImpl(String mapName) {
			map = db.getTreeMap(mapName);
		}

		@Override
		final public V get(K key) {
			return map.get(key);
		}

		@Override
		final public void put(K key, V value) {
			map.put(key, value);
		}

		@Override
		final public void remove(K key) {
			map.remove(key);
		}
	}

}
