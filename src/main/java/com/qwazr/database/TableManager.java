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
 */
package com.qwazr.database;

import com.qwazr.database.model.TableDefinition;
import com.qwazr.utils.LockUtils;
import com.qwazr.utils.server.ServerException;
import org.apache.commons.io.filefilter.FileFilterUtils;

import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TableManager {

	private final LockUtils.ReadWriteLock rwl = new LockUtils.ReadWriteLock();

	public static volatile TableManager INSTANCE = null;

	public File directory;

	public final ExecutorService executor;

	public static void load(File directory) throws IOException,
			URISyntaxException, ServerException {
		if (INSTANCE != null)
			throw new IOException("Already loaded");
		INSTANCE = new TableManager(directory);
	}

	private TableManager(File directory) throws ServerException, IOException {
		this.directory = directory;
		executor = Executors.newFixedThreadPool(8);
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				executor.shutdown();
			}
		});
	}

	private Table getTable(String tableName, boolean createIfNotExist)
			throws IOException, ServerException, DatabaseException {
		File dbDirectory = new File(directory, tableName);
		if (!dbDirectory.exists()) {
			if (!createIfNotExist)
				throw new ServerException(Response.Status.NOT_FOUND, "Table not found: " + tableName);
			dbDirectory.mkdir();
			if (!dbDirectory.exists())
				throw new ServerException(Response.Status.INTERNAL_SERVER_ERROR,
						"The directory cannot be created: " + dbDirectory.getAbsolutePath());

		}
		return Table.getInstance(dbDirectory, true);
	}

	public Set<String> getNameSet() {
		rwl.r.lock();
		try {
			LinkedHashSet<String> names = new LinkedHashSet<String>();
			for (File file : directory.listFiles((FileFilter) FileFilterUtils.directoryFileFilter()))
				if (!file.isHidden())
					names.add(file.getName());
			return names;
		} finally {
			rwl.r.unlock();
		}
	}

	public void createUpdateTable(String tableName, TableDefinition tableDefinition)
			throws IOException, ServerException {
		rwl.w.lock();
		try {
			Table table = getTable(tableName, true);
			table.setColumns(tableDefinition.columns);
		} catch (Exception e) {
			throw ServerException.getServerException(e);
		} finally {
			rwl.w.unlock();
		}
	}

	public TableDefinition getTableDefinition(String tableName) throws IOException, ServerException, DatabaseException {
		rwl.r.lock();
		try {
			return getTable(tableName, false).getTableDefinition();
		} finally {
			rwl.r.unlock();
		}
	}

	public void delete(String tableName) throws ServerException,
			IOException {
		rwl.w.lock();
		try {
			Table.deleteTable(new File(directory, tableName));
		} finally {
			rwl.w.unlock();
		}
	}

	public void upsertRow(String tableName, String row_id, Map<String, Object> nodeMap)
			throws IOException, ServerException, DatabaseException {
		rwl.r.lock();
		try {
			Table table = getTable(tableName, false);
			table.upsertRow(row_id, nodeMap);
		} finally {
			rwl.r.unlock();
		}
	}

	public void upsertRows(String tableName, List<Map<String, Object>> rows)
			throws IOException, ServerException, DatabaseException {
		rwl.r.lock();
		try {
			Table table = getTable(tableName, false);
			for (Map<String, Object> row : rows)
				table.upsertRow(null, row);
		} finally {
			rwl.r.unlock();
		}
	}

	public LinkedHashMap<String, Object> getRow(String tableName, String key, Set<String> columns)
			throws IOException, ServerException, DatabaseException {
		rwl.r.lock();
		try {
			Table table = getTable(tableName, false);
			LinkedHashMap<String, Object> row = table.getRow(key, columns);
			if (row == null)
				throw new ServerException("Row not found: " + key);
			return row;
		} finally {
			rwl.r.unlock();
		}
	}

	public boolean deleteRow(String tableName, String key) throws IOException, ServerException, DatabaseException {
		rwl.r.lock();
		try {
			Table table = getTable(tableName, false);
			return table.deleteRow(key);
		} finally {
			rwl.r.unlock();
		}
	}


}
