/*
 * Copyright 2015-2016 Emmanuel Keller / QWAZR
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qwazr.database.store.keys;

import com.qwazr.database.store.ByteConverter;
import com.qwazr.database.store.KeyIterator;
import com.qwazr.database.store.KeyStore;
import com.qwazr.utils.ArrayUtils;
import com.qwazr.utils.concurrent.BiConsumerEx;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

public abstract class KeyAbstract<V> implements KeyInterface<V> {

	private final KeyEnum keyType;

	protected final ByteConverter<V> byteConverter;

	private byte[] keyBytes;

	protected KeyAbstract(final KeyEnum keyType, final ByteConverter<V> byteConverter) {
		this.keyType = keyType;
		this.byteConverter = byteConverter;
		this.keyBytes = null;
	}

	@Override
	public void buildKey(final DataOutputStream output) throws IOException {
		output.writeChar(keyType.id);
	}

	final public synchronized byte[] getCachedKey() throws IOException {
		if (keyBytes != null)
			return keyBytes;
		try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
			try (final DataOutputStream output = new DataOutputStream(baos)) {
				buildKey(output);
				output.flush();
				keyBytes = baos.toByteArray();
				return keyBytes;
			}
		}
	}

	@Override
	final public V getValue(final KeyStore store) throws IOException {
		byte[] bytes = store.get(getCachedKey());
		if (bytes == null)
			return null;
		return byteConverter.toValue(bytes);
	}

	@Override
	final public void setValue(final KeyStore store, final V value) throws IOException {
		store.put(getCachedKey(), byteConverter.toBytes(value));
	}

	@Override
	final public void deleteValue(final KeyStore store) throws IOException {
		store.delete(getCachedKey());
	}

	@Override
	final public void prefixedKeys(final KeyStore store, int start, int rows,
			final BiConsumerEx<byte[], byte[], IOException> consumer) throws IOException {
		final byte[] prefixKey = getCachedKey();
		try (final KeyIterator iterator = store.iterator(prefixKey)) {
			while (start-- > 0 && iterator.hasNext()) {
				final Map.Entry<byte[], byte[]> entry = iterator.next();
				if (!ArrayUtils.startsWith(entry.getKey(), prefixKey))
					return;
			}
			while (rows-- > 0 && iterator.hasNext()) {
				final Map.Entry<byte[], byte[]> entry = iterator.next();
				final byte[] key = entry.getKey();
				if (!ArrayUtils.startsWith(key, prefixKey))
					return;
				consumer.accept(key, entry.getValue());
			}
		}
	}

}