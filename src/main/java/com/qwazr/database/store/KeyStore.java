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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

public class KeyStore implements Closeable {

	private final DB db;
	private final File file;

	KeyStore(File file) throws IOException {
		final Options options = new Options();
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
