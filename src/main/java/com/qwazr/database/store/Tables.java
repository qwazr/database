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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Tables {

	private final static LockUtils.ReadWriteLock rwlTables = new LockUtils.ReadWriteLock();

	private final static Map<File, Table> tables = new HashMap<File, Table>();

	public static Table getInstance(File directory, boolean createIfNotExist) throws IOException, DatabaseException {
		rwlTables.r.lock();
		try {
			Table table = tables.get(directory);
			if (table != null)
				return table;
		} finally {
			rwlTables.r.unlock();
		}
		if (!createIfNotExist)
			return null;
		rwlTables.w.lock();
		try {
			Table table = tables.get(directory);
			if (table != null)
				return table;
			table = new Table(directory);
			tables.put(directory, table);
			return table;
		} finally {
			rwlTables.w.unlock();
		}
	}

	static void close(File directory) throws IOException {
		rwlTables.r.lock();
		try {
			Table table = tables.get(directory);
			if (table == null)
				return;
		} finally {
			rwlTables.r.unlock();
		}
		rwlTables.w.lock();
		try {
			Table table = tables.get(directory);
			if (table == null)
				return;
			tables.remove(directory);
		} finally {
			rwlTables.w.unlock();
		}
	}
}
