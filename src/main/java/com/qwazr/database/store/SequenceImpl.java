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

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.commons.lang3.NotImplementedException;

import java.io.IOException;

import static org.fusesource.leveldbjni.JniDBFactory.bytes;

public abstract class SequenceImpl<T> implements SequenceInterface<T> {

	protected final StoreImpl store;

	protected final byte[] bytesKey;

	private SequenceImpl(StoreImpl store, String sequenceName) {
		this.store = store;
		this.bytesKey = bytes("seq." + sequenceName);
	}

	static <T> SequenceInterface<?> newSequence(StoreImpl store, String sequenceName, Class<T> clazz) {
		if (Long.class == clazz)
			return new LongSequence(store, sequenceName);
		if (Integer.class == clazz)
			return new IntegerSequence(store, sequenceName);
		throw new NotImplementedException("Sequence type is not implemented: " + clazz.getSimpleName());
	}

	public static class LongSequence extends SequenceImpl<Long> {

		LongSequence(StoreImpl store, String sequenceName) {
			super(store, sequenceName);
		}

		@Override
		public Long incrementAndGet() throws IOException {
			synchronized (this) {
				byte[] bytes = store.get(bytesKey);
				long value = bytes == null ? 0 : Longs.fromByteArray(bytes);
				store.put(bytesKey, Longs.toByteArray(++value));
				return value;
			}
		}
	}

	public static class IntegerSequence extends SequenceImpl<Integer> {

		IntegerSequence(StoreImpl store, String sequenceName) {
			super(store, sequenceName);
		}

		@Override
		public Integer incrementAndGet() throws IOException {
			synchronized (this) {
				byte[] bytes = store.get(bytesKey);
				int value = bytes == null ? 0 : Ints.fromByteArray(bytes);
				store.put(bytesKey, Ints.toByteArray(++value));
				return value;
			}
		}
	}
}
