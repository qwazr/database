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
 */
package com.qwazr.database;

import com.qwazr.database.model.ColumnDefinition;
import com.qwazr.database.model.TableRequest;
import com.qwazr.database.model.TableRequestResult;
import com.qwazr.database.store.*;
import com.qwazr.utils.LockUtils;
import com.qwazr.utils.server.ServerBuilder;
import com.qwazr.utils.server.ServerException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.roaringbitmap.RoaringBitmap;

import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;

public class TableManager {

	public final static String SERVICE_NAME_TABLE = "table";

	private final LockUtils.ReadWriteLock rwl = new LockUtils.ReadWriteLock();

	static volatile TableManager INSTANCE = null;

	public File directory;

	public final ExecutorService executor;

	public static synchronized void load(final ServerBuilder serverBuilder) throws IOException {
		if (INSTANCE != null)
			throw new IOException("Already loaded");
		File tableDir = new File(serverBuilder.getServerConfiguration().dataDirectory, SERVICE_NAME_TABLE);
		if (!tableDir.exists())
			tableDir.mkdir();
		try {
			INSTANCE = new TableManager(serverBuilder.getExecutorService(), tableDir);
			if (serverBuilder != null) {
				serverBuilder.registerWebService(TableServiceImpl.class);
				serverBuilder.registerShutdownListener(server -> Tables.closeAll());
			}
		} catch (ServerException e) {
			throw new RuntimeException(e);
		}
	}

	public static TableManager getInstance() {
		if (INSTANCE == null)
			throw new RuntimeException("The table service is not enabled");
		return INSTANCE;
	}

	private TableManager(final ExecutorService executor, final File directory) throws ServerException, IOException {
		this.directory = directory;
		this.executor = executor;
	}

	private Table getTable(final String tableName) throws IOException {
		File dbDirectory = new File(directory, tableName);
		if (!dbDirectory.exists())
			throw new ServerException(Response.Status.NOT_FOUND, "Table not found: " + tableName);
		return Tables.getInstance(dbDirectory, true);
	}

	public void createTable(final String tableName) throws IOException {
		rwl.writeEx(() -> {
			final File dbDirectory = new File(directory, tableName);
			if (dbDirectory.exists())
				throw new ServerException(Response.Status.CONFLICT, "The table already exists: " + tableName);
			dbDirectory.mkdir();
			if (!dbDirectory.exists())
				throw new ServerException(Response.Status.INTERNAL_SERVER_ERROR,
						"The directory cannot be created: " + dbDirectory.getAbsolutePath());
			Tables.getInstance(dbDirectory, true);
		});
	}

	public Set<String> getNameSet() {
		return rwl.read(() -> {
			final LinkedHashSet<String> names = new LinkedHashSet<>();
			for (File file : directory.listFiles((FileFilter) FileFilterUtils.directoryFileFilter()))
				if (!file.isHidden())
					names.add(file.getName());
			return names;
		});
	}

	public Map<String, ColumnDefinition> getColumns(final String tableName) throws IOException {
		return rwl.readEx(() -> getTable(tableName).getColumns());
	}

	public void setColumn(final String tableName, final String columnName, final ColumnDefinition columnDefinition)
			throws IOException {
		rwl.writeEx(() -> {
			getTable(tableName).setColumn(columnName, columnDefinition);
		});
	}

	public void removeColumn(final String tableName, final String columnName) throws IOException {
		rwl.writeEx(() -> {
			getTable(tableName).removeColumn(columnName);
		});
	}

	public void deleteTable(final String tableName) throws IOException {
		rwl.writeEx(() -> {
			final File dbDirectory = new File(directory, tableName);
			Table table = Tables.getInstance(dbDirectory, false);
			if (table != null)
				table.close();
			if (!dbDirectory.exists())
				throw new ServerException(Response.Status.NOT_FOUND, "Table not found: " + tableName);
			FileUtils.deleteDirectory(dbDirectory);
		});
	}

	public void upsertRow(final String tableName, final String row_id, final Map<String, Object> nodeMap)
			throws IOException {
		rwl.readEx(() -> {
			return getTable(tableName).upsertRow(row_id, nodeMap);
		});
	}

	public int upsertRows(final String tableName, final List<Map<String, Object>> rows) throws IOException {
		return rwl.readEx(() -> {
			return getTable(tableName).upsertRows(rows);
		});
	}

	public LinkedHashMap<String, Object> getRow(final String tableName, final String key, final Set<String> columns)
			throws IOException {
		return rwl.readEx(() -> {
			final Table table = getTable(tableName);
			final LinkedHashMap<String, Object> row = table.getRow(key, columns);
			if (row == null)
				throw new ServerException(Response.Status.NOT_FOUND, "Row not found: " + key);
			return row;
		});
	}

	public List<String> getPrimaryKeys(final String tableName, final Integer start, final Integer rows)
			throws IOException {
		return rwl.readEx(() -> {
			return getTable(tableName).getPrimaryKeys(start == null ? 0 : start, rows == null ? 10 : rows);
		});
	}

	public boolean deleteRow(final String tableName, final String key) throws IOException {
		return rwl.readEx(() -> {
			return getTable(tableName).deleteRow(key);
		});
	}

	public TableRequestResult query(final String tableName, final TableRequest request) throws IOException {
		return rwl.readEx(() -> {

			final long start = request.start == null ? 0 : request.start;
			final long rows = request.rows == null ? Long.MAX_VALUE : request.rows;

			final Table table = getTable(tableName);

			final Map<String, Map<String, CollectorInterface.LongCounter>> counters;
			if (request.counters != null && !request.counters.isEmpty()) {
				counters = new LinkedHashMap<>();
				request.counters.forEach(col -> counters.put(col, new HashMap<>()));
			} else
				counters = null;

			final Query query = request.query == null ? null : Query.prepare(request.query, null);

			final RoaringBitmap docBitset = table.query(query, counters).finalBitmap;

			if (docBitset == null || docBitset.isEmpty())
				return new TableRequestResult(0L);

			final long count = docBitset.getCardinality();
			final TableRequestResult result = new TableRequestResult(count);

			table.getRows(docBitset, request.columns, start, rows, result.rows);

			if (counters == null)
				return result;

			counters.forEach((countersEntryKey, countersEntry) -> {
				final LinkedHashMap<String, Long> counter = new LinkedHashMap<>();
				countersEntry.forEach((key, counterEntry) -> {
					counter.put(key, counterEntry.count);
				});
				result.counters.put(countersEntryKey, counter);
			});

			return result;
		});
	}

}
