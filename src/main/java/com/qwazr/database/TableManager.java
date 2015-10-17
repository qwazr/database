/**
 * Copyright 2015 Emmanuel Keller / QWAZR
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
package com.qwazr.database;

import com.qwazr.database.model.ColumnDefinition;
import com.qwazr.database.model.TableRequest;
import com.qwazr.database.model.TableRequestResult;
import com.qwazr.database.store.*;
import com.qwazr.utils.LockUtils;
import com.qwazr.utils.server.ServerException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.roaringbitmap.RoaringBitmap;

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

	public static void load(File directory) throws IOException, URISyntaxException, ServerException {
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

	private Table getTable(String tableName) throws IOException, ServerException, DatabaseException {
		File dbDirectory = new File(directory, tableName);
		if (!dbDirectory.exists())
			throw new ServerException(Response.Status.NOT_FOUND, "Table not found: " + tableName);
		return Tables.getInstance(dbDirectory, true);
	}

	public void createTable(String tableName) throws ServerException, IOException, DatabaseException {
		rwl.w.lock();
		try {
			File dbDirectory = new File(directory, tableName);
			if (dbDirectory.exists())
				throw new ServerException(Response.Status.CONFLICT, "The table already exists: " + tableName);
			dbDirectory.mkdir();
			if (!dbDirectory.exists())
				throw new ServerException(Response.Status.INTERNAL_SERVER_ERROR,
								"The directory cannot be created: " + dbDirectory.getAbsolutePath());
			Tables.getInstance(dbDirectory, true);
		} finally {
			rwl.w.unlock();
		}
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

	public Map<String, ColumnDefinition> getColumns(String tableName)
					throws ServerException, DatabaseException, IOException {
		rwl.r.lock();
		try {
			return getTable(tableName).getColumns();
		} finally {
			rwl.r.unlock();
		}
	}

	public void addColumn(String tableName, String columnName, ColumnDefinition columnDefinition)
					throws IOException, ServerException {
		rwl.w.lock();
		try {
			Table table = getTable(tableName);
			table.addColumn(columnName, columnDefinition);
		} catch (Exception e) {
			throw ServerException.getServerException(e);
		} finally {
			rwl.w.unlock();
		}
	}

	public void removeColumn(String tableName, String columnName) throws IOException, ServerException {
		rwl.w.lock();
		try {
			Table table = getTable(tableName);
			table.removeColumn(columnName);
		} catch (Exception e) {
			throw ServerException.getServerException(e);
		} finally {
			rwl.w.unlock();
		}
	}

	public void deleteTable(String tableName) throws ServerException, IOException, DatabaseException {
		rwl.w.lock();
		try {
			File dbDirectory = new File(directory, tableName);
			Table table = Tables.getInstance(dbDirectory, false);
			if (table != null)
				table.close();
			if (!dbDirectory.exists())
				throw new ServerException(Response.Status.NOT_FOUND, "Table not found: " + tableName);
			FileUtils.deleteDirectory(dbDirectory);
		} finally {
			rwl.w.unlock();
		}
	}

	public void upsertRow(String tableName, String row_id, Map<String, Object> nodeMap)
					throws IOException, ServerException, DatabaseException {
		rwl.r.lock();
		try {
			Table table = getTable(tableName);
			table.upsertRow(row_id, nodeMap);
		} finally {
			rwl.r.unlock();
		}
	}

	public int upsertRows(String tableName, List<Map<String, Object>> rows)
					throws IOException, ServerException, DatabaseException {
		rwl.r.lock();
		try {
			Table table = getTable(tableName);
			return table.upsertRows(rows);
		} finally {
			rwl.r.unlock();
		}
	}

	public LinkedHashMap<String, Object> getRow(String tableName, String key, Set<String> columns)
					throws IOException, ServerException, DatabaseException {
		rwl.r.lock();
		try {
			Table table = getTable(tableName);
			LinkedHashMap<String, Object> row = table.getRow(key, columns);
			if (row == null)
				throw new ServerException(Response.Status.NOT_FOUND, "Row not found: " + key);
			return row;
		} finally {
			rwl.r.unlock();
		}
	}

	public boolean deleteRow(String tableName, String key) throws IOException, ServerException, DatabaseException {
		rwl.r.lock();
		try {
			Table table = getTable(tableName);
			return table.deleteRow(key);
		} finally {
			rwl.r.unlock();
		}
	}

	public TableRequestResult query(String tableName, TableRequest request)
					throws ServerException, DatabaseException, IOException {
		rwl.r.lock();
		try {

			long start = request.start == null ? 0 : request.start;
			long rows = request.rows == null ? Long.MAX_VALUE : request.rows;

			Table table = getTable(tableName);

			if (request.query == null)
				throw new ServerException(Response.Status.NOT_ACCEPTABLE, "The query part is missing");

			Map<String, Map<String, CollectorInterface.LongCounter>> counters = null;
			if (request.counters != null && !request.counters.isEmpty()) {
				counters = new LinkedHashMap<String, Map<String, CollectorInterface.LongCounter>>();
				for (String col : request.counters) {
					Map<String, CollectorInterface.LongCounter> termCount = new HashMap<String, CollectorInterface.LongCounter>();
					counters.put(col, termCount);
				}
			}

			Query query = Query.prepare(request.query, null);

			RoaringBitmap docBitset = table.query(query, counters).finalBitmap;

			if (docBitset == null || docBitset.isEmpty())
				return new TableRequestResult(null);

			long count = docBitset.getCardinality();
			TableRequestResult result = new TableRequestResult(count);

			table.getRows(docBitset, request.columns, start, rows, result.rows);

			if (counters != null) {
				for (Map.Entry<String, Map<String, CollectorInterface.LongCounter>> countersEntry : counters
								.entrySet()) {
					LinkedHashMap<String, Long> counter = new LinkedHashMap<String, Long>();
					for (Map.Entry<String, CollectorInterface.LongCounter> counterEntry : countersEntry.getValue()
									.entrySet())
						counter.put(counterEntry.getKey(), counterEntry.getValue().count);
					result.counters.put(countersEntry.getKey(), counter);
				}
			}

			return result;
		} finally {
			rwl.r.unlock();
		}

	}

}
