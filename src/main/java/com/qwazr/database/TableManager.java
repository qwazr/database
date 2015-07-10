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
import com.qwazr.utils.json.DirectoryJsonManager;
import com.qwazr.utils.server.ServerException;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class TableManager extends DirectoryJsonManager<TableDefinition> {

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
		super(directory, TableDefinition.class);
		this.directory = directory;
		executor = Executors.newFixedThreadPool(8);
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				executor.shutdown();
			}
		});
	}

	private Table getTable(String tableName) throws IOException, ServerException {
		File dbDirectory = new File(directory, tableName);
		if (!dbDirectory.exists())
			dbDirectory.mkdir();
		return Table.getInstance(dbDirectory);
	}

	@Override
	public Set<String> nameSet() {
		return super.nameSet();
	}

	@Override
	public TableDefinition get(String name) {
		return super.get(name);
	}

	public void createUpdateTable(String tableName, TableDefinition tableDefinition)
			throws IOException, ServerException {
		rwl.w.lock();
		try {
			super.set(tableName, tableDefinition);
			Table table = getTable(tableName);

			Set<String> columnLeft = new HashSet<String>();
			table.collectExistingColumns(columnLeft);
			AtomicBoolean needCommit = new AtomicBoolean(false);

			if (tableDefinition.columns != null)
				table.setColumns(tableDefinition.columns, columnLeft, needCommit);

			if (columnLeft.size() > 0) {
				needCommit.set(true);
				for (String columnName : columnLeft)
					table.removeColumn(columnName);
			}

			if (needCommit.get())
				table.commit();

		} catch (Exception e) {
			throw ServerException.getServerException(e);
		} finally {
			rwl.w.unlock();
		}
	}

	@Override
	public TableDefinition delete(String tableName) throws ServerException,
			IOException {
		rwl.w.lock();
		try {
			TableDefinition tableDef = super.delete(tableName);
			Table.deleteTable(new File(directory, tableName));
			return tableDef;
		} finally {
			rwl.w.unlock();
		}
	}

}
