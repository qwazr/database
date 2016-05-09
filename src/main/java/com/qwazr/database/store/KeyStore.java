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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

public class KeyStore implements Closeable {

	private static final Logger logger = LoggerFactory.getLogger(KeyStore.class);

	private final DB db;
	private final File file;

	private static boolean MEMORY_POOL = false;

	final static void checkMemoryPool() {
		synchronized (KeyStore.class) {
			if (MEMORY_POOL)
				return;
			JniDBFactory.pushMemoryPool(1024 * 512);
			MEMORY_POOL = true;
		}
	}

	//TODO
	final static void freeMemoryPool() {
		synchronized (KeyStore.class) {
			if (!MEMORY_POOL)
				return;
			JniDBFactory.popMemoryPool();
			MEMORY_POOL = false;
		}
	}

	KeyStore(File file) throws IOException {
		checkMemoryPool();
		final Options options = new Options();
		options.logger(logger::info);
		this.file = file;
		options.createIfMissing(true);
		db = JniDBFactory.factory.open(file, options);
	}

	@Override
	public void close() throws IOException {
		db.close();
	}

	public boolean exists() {
		return file.exists();
	}

	public void delete() throws IOException {
		Options options = new Options();
		JniDBFactory.factory.destroy(file, options);
	}

	public byte[] get(byte[] key) {
		return db.get(key);
	}

	public void put(byte[] key, byte[] value) throws IOException {
		db.put(key, value);
	}

	public void delete(byte[] key) throws IOException {
		db.delete(key);
	}

	public DBIterator iterator() {
		return db.iterator();
	}
}
