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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

public interface KeyStore extends Closeable {

	boolean exists();

	void delete() throws IOException;

	byte[] get(byte[] key);

	void put(byte[] key, byte[] value) throws IOException;

	void delete(byte[] key) throws IOException;

	KeyIterator iterator(byte[] key);

	enum Impl {

		leveldb(KeyStoreLevelDb.class, "storedb"), lmdb(KeyStoreLmdb.class, "lmdb");

		final public Class<? extends KeyStore> storeClass;
		final public String directoryName;

		Impl(Class<? extends KeyStore> storeClass, final String directoryName) {
			this.storeClass = storeClass;
			this.directoryName = directoryName;
		}

		final public static Impl detect(final File directory) {
			if (!directory.exists())
				return null;
			for (Impl impl : values())
				if (new File(directory, impl.directoryName).exists())
					return impl;
			return null;
		}
	}
}
