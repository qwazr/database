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
import org.iq80.leveldb.DB;

import static org.fusesource.leveldbjni.JniDBFactory.bytes;

public abstract class SequenceImpl<T> implements SequenceInterface<T> {

	protected final DB db;

	protected final byte[] bytesKey;

	private SequenceImpl(DB db, String sequenceName) {
		this.db = db;
		this.bytesKey = bytes("seq." + sequenceName);
	}

	static <T> SequenceInterface<?> newSequence(DB db, String sequenceName, Class<T> clazz) {
		if (Long.class == clazz)
			return new LongSequence(db, sequenceName);
		if (Integer.class == clazz)
			return new IntegerSequence(db, sequenceName);
		throw new NotImplementedException("Sequence type is not implemented: " + clazz.getSimpleName());
	}

	public static class LongSequence extends SequenceImpl<Long> {

		LongSequence(DB db, String sequenceName) {
			super(db, sequenceName);
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

	public static class IntegerSequence extends SequenceImpl<Integer> {

		IntegerSequence(DB db, String sequenceName) {
			super(db, sequenceName);
		}

		@Override
		public Integer incrementAndGet() {
			synchronized (this) {
				byte[] bytes = db.get(bytesKey);
				int value = bytes == null ? 0 : Ints.fromByteArray(bytes);
				db.put(bytesKey, Ints.toByteArray(++value));
				return value;
			}
		}
	}
}
