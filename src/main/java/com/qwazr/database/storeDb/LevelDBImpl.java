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


import com.google.common.primitives.Longs;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.fusesource.leveldbjni.JniDBFactory.bytes;

public class LevelDBImpl implements StoreInterface {

	private final DB db;
	private volatile Map<String, StoreMapImpl> mapsCache;
	private final Map<String, StoreMapImpl> maps;
	private volatile Map<String, LongSequenceImpl> sequencesCache;
	private final Map<String, LongSequenceImpl> sequences;

	public LevelDBImpl(File file) throws IOException {
		Options options = new Options();
		options.createIfMissing(true);
		db = JniDBFactory.factory.open(file, options);
		maps = new HashMap<String, StoreMapImpl>();
		mapsCache = new HashMap<String, StoreMapImpl>();
		sequences = new HashMap<String, LongSequenceImpl>();
		sequencesCache = new HashMap<String, LongSequenceImpl>();
	}

	@Override
	public void close() throws IOException {
		db.close();
	}

	@Override
	public <K, V> StoreMap<K, V> getMap(String mapName, ByteConverter<K> keyConverter, ByteConverter<V> valueConverter) {
		StoreMapImpl<K, V> map = mapsCache.get(mapName);
		if (map != null)
			return map;
		synchronized (maps) {
			map = maps.get(mapName);
			if (map != null)
				return map;
			map = new StoreMapImpl<K, V>(mapName, valueConverter);
			maps.put(mapName, map);
			mapsCache = new HashMap<String, StoreMapImpl>(maps);
			return map;
		}
	}

	@Override
	public LongSequence getLongSequence(String sequenceName) {
		LongSequenceImpl sequence = sequencesCache.get(sequenceName);
		if (sequence != null)
			return sequence;
		synchronized (sequences) {
			sequence = sequences.get(sequenceName);
			if (sequence != null)
				return sequence;
			sequence = new LongSequenceImpl(sequenceName);
			sequences.put(sequenceName, sequence);
			sequencesCache = new HashMap<String, LongSequenceImpl>(sequences);
			return sequence;
		}
	}

	@Override
	public void commit() {
	}

	@Override
	public void delete(String collectionName) {
		synchronized (maps) {
			if (maps.remove(collectionName) == null)
				return;
			mapsCache = new HashMap<String, StoreMapImpl>(maps);
			//TODO delete keys
		}
	}

	@Override
	public boolean exists(String collectionName) {
		return mapsCache.containsKey(collectionName);
	}

	@Override
	public void rollback() {
	}

	private class LongSequenceImpl implements LongSequence {

		private final byte[] bytesKey;

		private LongSequenceImpl(String sequenceName) {
			bytesKey = bytes("seq." + sequenceName);
		}

		@Override
		public Long incrementAndGet() {
			synchronized (this) {
				byte[] bytes = db.get(bytesKey);
				long value = bytes == null ? 0 : Longs.fromByteArray(bytes);
				db.put(bytesKey, Longs.toByteArray(++value));
				return value;
			}
		}
	}

	private class StoreMapImpl<K, V> implements StoreMap<K, V> {

		private final String keyPrefix;
		private final ByteConverter<V> valueConverter;

		private StoreMapImpl(String mapName, ByteConverter<V> valueConverter) {
			keyPrefix = "map." + mapName + ".";
			this.valueConverter = valueConverter;
		}

		final protected byte[] getKey(K key) {
			return bytes(keyPrefix + key.toString());
		}

		@Override
		final public V get(K key) {
			byte[] bytes = db.get(getKey(key));
			if (bytes == null)
				return null;
			return valueConverter.toValue(bytes);
		}

		@Override
		final public void put(K key, V value) {
			db.put(getKey(key), valueConverter.toBytes(value));
		}

		@Override
		final public void remove(K key) {
			db.delete(getKey(key));
		}
	}

}
