/**
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
 **/
package com.qwazr.database.store;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

class KeyStoreLevelDb implements KeyStore {

	private static final Logger LOGGER = LoggerFactory.getLogger(KeyStoreLevelDb.class);

	private final DB db;
	private final File file;

	private static boolean MEMORY_POOL = false;

	final static void checkMemoryPool() {
		synchronized (KeyStoreLevelDb.class) {
			if (MEMORY_POOL)
				return;
			JniDBFactory.pushMemoryPool(1024 * 512);
			MEMORY_POOL = true;
		}
	}

	//TODO
	final static void freeMemoryPool() {
		synchronized (KeyStoreLevelDb.class) {
			if (!MEMORY_POOL)
				return;
			JniDBFactory.popMemoryPool();
			MEMORY_POOL = false;
		}
	}

	public KeyStoreLevelDb(final File file) throws IOException {
		checkMemoryPool();
		final Options options = new Options();
		options.logger(LOGGER::info);
		this.file = file;
		options.createIfMissing(true);
		db = JniDBFactory.factory.open(file, options);
	}

	@Override
	final public void close() throws IOException {
		db.close();
	}

	@Override
	final public boolean exists() {
		return file.exists();
	}

	@Override
	final public void delete() throws IOException {
		Options options = new Options();
		JniDBFactory.factory.destroy(file, options);
	}

	@Override
	final public byte[] get(final byte[] key) {
		return db.get(key);
	}

	@Override
	final public void put(final byte[] key, final byte[] value) {
		db.put(key, value);
	}

	@Override
	final public void delete(final byte[] key) {
		db.delete(key);
	}

	@Override
	final public KeyIterator iterator(final byte[] key) {
		return new KeyIteratorImpl(key);
	}

	private class KeyIteratorImpl implements KeyIterator {

		private final DBIterator iterator;

		private KeyIteratorImpl(final byte[] key) {
			this.iterator = db.iterator();
			this.iterator.seek(key);
		}

		@Override
		final public void close() throws IOException {
			iterator.close();
		}

		@Override
		final public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		final public Map.Entry<byte[], byte[]> next() {
			return iterator.next();
		}
	}
}
