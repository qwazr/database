/*
 * Copyright 2015-2017 Emmanuel Keller / QWAZR
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qwazr.database.store;

import com.qwazr.server.ServerException;
import com.qwazr.utils.LoggerUtils;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Tables {

	private static final Logger LOGGER = LoggerUtils.getLogger(Table.class);

	private final static ConcurrentHashMap<File, Table> tables = new ConcurrentHashMap<>();

	public static Table getInstance(final File directory, final KeyStore.Impl storeImpl) {
		return tables.computeIfAbsent(directory, file -> {
			final KeyStore.Impl si = storeImpl == null ? KeyStore.Impl.detect(directory) : storeImpl;
			if (si == null)
				throw new ServerException("Cannot detect the store type: " + directory);
			try {
				return new Table(file, si);
			} catch (IOException e) {
				throw ServerException.of(e);
			}
		});
	}

	static public void delete(final File directory) throws IOException {
		final Table table = tables.get(directory);
		if (table == null)
			return;
		table.close();
		table.delete();
	}

	static synchronized void close(final File directory) throws IOException {
		final Table table = tables.remove(directory);
		if (table == null)
			return;
		table.closeNoLock();
	}

	public static void closeAll() {
		tables.forEach((file, table) -> {
			try {
				table.closeNoLock();
			} catch (IOException e) {
				LOGGER.log(Level.WARNING, e, () -> "Cannot clause the table: " + table);
			}
		});
		tables.clear();
	}
}
