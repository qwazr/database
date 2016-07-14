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

import com.qwazr.utils.IOUtils;
import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.EntryIterator;
import org.fusesource.lmdbjni.Env;
import org.fusesource.lmdbjni.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

class KeyStoreLmdb implements KeyStore {

	private static final Logger LOGGER = LoggerFactory.getLogger(KeyStoreLmdb.class);

	private final Env env;
	private final Database db;
	private final File file;

	public KeyStoreLmdb(final File file) throws IOException {
		this.file = file;
		if (!file.exists())
			file.mkdir();
		this.env = new Env(file.getAbsolutePath());
		this.db = env.openDatabase();
	}

	@Override
	final public void close() throws IOException {
		IOUtils.close(db, env);
	}

	@Override
	final public boolean exists() {
		return file.exists();
	}

	@Override
	final public void delete() throws IOException {
		env.close();
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
	final public void delete(final byte[] key) throws IOException {
		db.delete(key);
	}

	@Override
	final public KeyIterator iterator(final byte[] key) {
		return new KeyIteratorImpl(key);
	}

	private class KeyIteratorImpl implements KeyIterator {

		private final Transaction tx;
		private final EntryIterator it;

		private KeyIteratorImpl(byte[] key) {
			tx = env.createReadTransaction();
			it = db.seek(tx, key);
		}

		@Override
		final public void close() throws IOException {
			IOUtils.close(it, tx);
		}

		@Override
		final public boolean hasNext() {
			return it.hasNext();
		}

		@Override
		final public Map.Entry<byte[], byte[]> next() {
			return it.next();
		}
	}
}
