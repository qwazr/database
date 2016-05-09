/**
 * Copyright 2015-2016 Emmanuel Keller / QWAZR
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

import com.qwazr.utils.LockUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Tables {

	private final static LockUtils.ReadWriteLock rwlTables = new LockUtils.ReadWriteLock();

	private static final Logger logger = LoggerFactory.getLogger(Table.class);

	private final static Map<File, Table> tables = new HashMap<>();

	public static Table getInstance(final File directory, final boolean createIfNotExist) throws IOException {
		final Table t = rwlTables.readEx(() -> tables.get(directory));
		if (t != null)
			return t;
		if (!createIfNotExist)
			return null;
		return rwlTables.writeEx(() -> {
			Table table = tables.get(directory);
			if (table != null)
				return table;
			table = new Table(directory);
			tables.put(directory, table);
			return table;
		});
	}

	static void close(final File directory) throws IOException {
		Table t = rwlTables.readEx(() -> tables.get(directory));
		if (t == null)
			return;
		rwlTables.writeEx(() -> {
			Table table = tables.get(directory);
			if (table == null)
				return;
			tables.remove(directory);
		});
	}

	public static void closeAll() {
		rwlTables.writeEx(() -> {
			tables.forEach((file, table) -> {
				try {
					table.closeNoLock();
				} catch (IOException e) {
					logger.warn("Cannot clause the table: " + table, e);
				}
			});
			tables.clear();
		});
	}
}
