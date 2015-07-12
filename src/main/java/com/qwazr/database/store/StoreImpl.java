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
package com.qwazr.database.store;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class StoreImpl implements StoreInterface {

	private final DB db;
	private volatile Map<String, StoreMapInterface> mapsCache;
	private final Map<String, StoreMapInterface> maps;
	private volatile Map<String, SequenceInterface> sequencesCache;
	private final Map<String, SequenceInterface> sequences;

	public StoreImpl(File file) throws IOException {
		Options options = new Options();
		options.createIfMissing(true);
		db = JniDBFactory.factory.open(file, options);
		maps = new HashMap<String, StoreMapInterface>();
		mapsCache = new HashMap<String, StoreMapInterface>();
		sequences = new HashMap<String, SequenceInterface>();
		sequencesCache = new HashMap<String, SequenceInterface>();
	}

	@Override
	public void close() throws IOException {
		db.close();
	}

	@Override
	public <K, V> StoreMapInterface<K, V> getMap(String mapName, ByteConverter<K> keyConverter,
												 ByteConverter<V> valueConverter) {
		StoreMapInterface<K, V> map = mapsCache.get(mapName);
		if (map != null)
			return map;
		synchronized (maps) {
			map = maps.get(mapName);
			if (map != null)
				return map;
			map = new StoreMapImpl<K, V>(db, mapName, keyConverter, valueConverter);
			maps.put(mapName, map);
			mapsCache = new HashMap<String, StoreMapInterface>(maps);
			return map;
		}
	}

	@Override
	public <T> SequenceInterface<T> getSequence(String sequenceName, Class<T> clazz) {
		SequenceInterface<T> sequence = sequencesCache.get(sequenceName);
		if (sequence != null)
			return sequence;
		synchronized (sequences) {
			sequence = sequences.get(sequenceName);
			if (sequence != null)
				return sequence;
			sequence = (SequenceInterface<T>) SequenceImpl.newSequence(db, sequenceName, clazz);
			sequences.put(sequenceName, sequence);
			sequencesCache = new HashMap<String, SequenceInterface>(sequences);
			return sequence;
		}
	}

	@Override
	public void delete(String collectionName) {
		synchronized (maps) {
			if (maps.remove(collectionName) == null)
				return;
			mapsCache = new HashMap<String, StoreMapInterface>(maps);
			//TODO delete keys
		}
	}

	@Override
	public boolean exists(String collectionName) {
		return mapsCache.containsKey(collectionName);
	}

}
