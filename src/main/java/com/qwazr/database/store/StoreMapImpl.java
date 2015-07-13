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


import com.qwazr.utils.ArrayUtils;
import org.iq80.leveldb.DBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.fusesource.leveldbjni.JniDBFactory.bytes;

class StoreMapImpl<K, V> implements StoreMapInterface<K, V> {

	private static final Logger logger = LoggerFactory.getLogger(StoreMapImpl.class);

	private final StoreImpl store;
	private final String keyPrefix;
	private final byte[] keyPrefixBytes;
	private final ByteConverter<K> keyConverter;
	private final ByteConverter<V> valueConverter;

	StoreMapImpl(StoreImpl store, String mapName, ByteConverter<K> keyConverter, ByteConverter<V> valueConverter) {
		this.store = store;
		this.keyPrefix = "map." + mapName + ".";
		this.keyPrefixBytes = bytes(keyPrefix);
		this.keyConverter = keyConverter;
		this.valueConverter = valueConverter;
		if (logger.isDebugEnabled())
			logger.debug("New StoreMapImpl: " + keyPrefix);
	}

	final protected byte[] getKey(K key) {
		return bytes(keyPrefix + key.toString());
	}

	@Override
	final public V get(K key) throws IOException {
		byte[] bytes = store.get(getKey(key));
		if (bytes == null)
			return null;
		return valueConverter.toValue(bytes);
	}

	@Override
	final public void put(K key, V value) throws IOException {
		if (logger.isDebugEnabled())
			logger.debug("Put key: " + keyPrefix + key);
		byte[] keyBytes = getKey(key);
		byte[] bytes = valueConverter.toBytes(value);
		if (bytes == null)
			store.delete(keyBytes);
		else
			store.put(keyBytes, bytes);
	}

	@Override
	final public void delete(K key) throws IOException {
		if (logger.isDebugEnabled())
			logger.debug("Delete key: " + keyPrefix + key);
		store.delete(getKey(key));
	}

	final public Iterator<Map.Entry<K, V>> iterator() {
		try {
			return new KeyIterator();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private class KeyIterator implements Iterator<Map.Entry<K, V>>, Closeable {

		private final DBIterator dbIterator;
		private Map.Entry<K, V> nextEntry;

		private KeyIterator() throws IOException {
			dbIterator = store.iterator();
			dbIterator.seek(keyPrefixBytes);
			nextEntry = forward();
		}

		private Map.Entry<K, V> forward() throws IOException {
			if (!dbIterator.hasNext())
				return null;
			Map.Entry<byte[], byte[]> entry = dbIterator.next();
			if (entry == null) // should not occur (we called hasNext before)
				return null;
			byte[] keyBytes = entry.getKey();
			if (!ArrayUtils.startsWith(keyBytes, keyPrefixBytes))
				return null;
			K key = keyConverter.toValue(ArrayUtils.subarray(keyBytes, keyPrefixBytes.length, keyBytes.length));
			V value = valueConverter.toValue(entry.getValue());
			return new AbstractMap.SimpleEntry<K, V>(key, value);
		}

		public boolean hasNext() {
			return nextEntry != null;
		}

		@Override
		public Map.Entry<K, V> next() {
			if (nextEntry == null)
				throw new NoSuchElementException();
			Map.Entry<K, V> currentEntry = nextEntry;
			try {
				nextEntry = forward();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			return currentEntry;
		}

		@Override
		public void close() throws IOException {
			dbIterator.close();
		}
	}
}
