/*
 * Copyright 2015-2018 Emmanuel Keller / QWAZR
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

import com.fasterxml.jackson.core.type.TypeReference;
import com.qwazr.database.model.ColumnDefinition;
import com.qwazr.database.model.TableDefinition;
import com.qwazr.database.model.TableRequest;
import com.qwazr.database.model.TableRequestResult;
import com.qwazr.database.model.TableStatus;
import com.qwazr.database.store.KeyStore;
import com.qwazr.database.store.Query;
import com.qwazr.server.AbstractServiceImpl;
import com.qwazr.server.ServerException;
import com.qwazr.utils.LoggerUtils;
import com.qwazr.utils.ObjectMappers;

import javax.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.logging.Logger;

class TableServiceImpl extends AbstractServiceImpl implements TableServiceInterface {

	private static final Logger LOGGER = LoggerUtils.getLogger(TableServiceImpl.class);

	private TableManager tableManager;

	TableServiceImpl(TableManager tableManager) {
		this.tableManager = tableManager;
	}

	public TableServiceImpl() {
		this(null);
	}

	@PostConstruct
	public void init() {
		this.tableManager = getContextAttribute(TableManager.class);
	}

	@Override
	public SortedSet<String> list() {
		return Collections.unmodifiableSortedSet(tableManager.getNameSet());
	}

	@Override
	public TableDefinition createTable(final String tableName, KeyStore.Impl storeImplementation) {
		try {
			if (storeImplementation == null)
				storeImplementation = KeyStore.Impl.leveldb;
			tableManager.createTable(tableName, storeImplementation);
			return new TableDefinition(storeImplementation, tableManager.getColumns(tableName));
		} catch (Exception e) {
			throw ServerException.getJsonException(LOGGER, e);
		}
	}

	@Override
	public TableStatus getTableStatus(String tableName) {
		try {
			return tableManager.getStatus(tableName);
		} catch (IOException | ServerException e) {
			throw ServerException.getJsonException(LOGGER, e);
		}
	}

	@Override
	public Boolean deleteTable(String tableName) {
		try {
			tableManager.deleteTable(tableName);
			return true;
		} catch (IOException | ServerException e) {
			throw ServerException.getJsonException(LOGGER, e);
		}
	}

	@Override
	public Map<String, ColumnDefinition> getColumns(String tableName) {
		try {
			return tableManager.getColumns(tableName);
		} catch (ServerException | IOException e) {
			throw ServerException.getJsonException(LOGGER, e);
		}
	}

	@Override
	public ColumnDefinition getColumn(final String tableName, final String columnName) {
		try {
			return tableManager.getColumns(tableName).get(columnName);
		} catch (ServerException | IOException e) {
			throw ServerException.getJsonException(LOGGER, e);
		}
	}

	@Override
	public List<Object> getColumnTerms(final String tableName, final String columnName, final Integer start,
			final Integer rows) {
		try {
			return tableManager.getColumnTerms(tableName, columnName, start, rows);
		} catch (ServerException | IOException e) {
			throw ServerException.getJsonException(LOGGER, e);
		}
	}

	@Override
	public List<String> getColumnTermKeys(final String tableName, final String columnName, final String term,
			final Integer start, final Integer rows) {
		try {
			return tableManager.getColumnTermKeys(tableName, columnName, term, start, rows);
		} catch (ServerException | IOException e) {
			throw ServerException.getJsonException(LOGGER, e);
		}
	}

	@Override
	public ColumnDefinition setColumn(String tableName, String columnName, ColumnDefinition columnDefinition) {
		try {
			tableManager.setColumn(tableName, columnName, columnDefinition);
			return columnDefinition;
		} catch (ServerException | IOException e) {
			throw ServerException.getJsonException(LOGGER, e);
		}
	}

	@Override
	public Boolean removeColumn(String table_name, String column_name) {
		throw new RuntimeException("Not yet implemented");
	}

	@Override
	public Long upsertRows(String table_name, List<Map<String, Object>> rows) {
		try {
			return (long) tableManager.upsertRows(table_name, rows);
		} catch (IOException | ServerException e) {
			throw ServerException.getJsonException(LOGGER, e);
		}
	}

	public final static TypeReference<Map<String, Object>> MapStringColumnValueTypeRef =
			new TypeReference<Map<String, Object>>() {
			};

	private class BufferFlush {

		private final List<Map<String, Object>> buffer;
		private final String tableName;

		private BufferFlush(int bufferSize, String tableName) {
			this.buffer = new ArrayList<>(bufferSize);
			this.tableName = tableName;
		}

		private int addRow(Map<String, Object> row) {
			buffer.add(row);
			return buffer.size();
		}

		private int flush() throws IOException {
			if (buffer.size() == 0)
				return 0;
			int res = tableManager.upsertRows(tableName, buffer);
			buffer.clear();
			return res;
		}

	}

	@Override
	public Long upsertRows(final String table_name, final Integer bufferLength, final InputStream inputStream) {
		final int bufferSize = bufferLength == null || bufferLength < 1 ? 50 : bufferLength;

		try (final InputStreamReader irs = new InputStreamReader(inputStream, "UTF-8");
				final BufferedReader br = new BufferedReader(irs)) {
			long counter = 0;
			String line;
			final BufferFlush buffer = new BufferFlush(bufferSize, table_name);
			while ((line = br.readLine()) != null) {
				line = line.trim();
				if (line.isEmpty())
					continue;
				final Map<String, Object> nodeMap = ObjectMappers.JSON.readValue(line, MapStringColumnValueTypeRef);
				if (buffer.addRow(nodeMap) == bufferSize)
					counter += buffer.flush();
			}
			counter += buffer.flush();
			return counter;
		} catch (Exception e) {
			throw ServerException.getJsonException(LOGGER, e);
		}
	}

	@Override
	public Map<String, Object> upsertRow(String table_name, String row_id, Map<String, Object> node) {
		try {
			tableManager.upsertRow(table_name, row_id, node);
			return node;
		} catch (ServerException | IOException e) {
			throw ServerException.getJsonException(LOGGER, e);
		}
	}

	@Override
	public LinkedHashMap<String, Object> getRow(String table_name, String row_id, Set<String> columns) {
		try {
			return tableManager.getRow(table_name, row_id, columns);
		} catch (ServerException | IOException e) {
			throw ServerException.getJsonException(LOGGER, e);
		}
	}

	@Override
	public List<Map<String, Object>> getRows(String table_name, Set<String> columns, Set<String> row_ids) {
		try {
			return tableManager.getRows(table_name, columns, row_ids);
		} catch (ServerException | IOException e) {
			throw ServerException.getJsonException(LOGGER, e);
		}
	}

	@Override
	public List<String> getRows(String table_name, Integer start, Integer rows) {
		try {
			return tableManager.getPrimaryKeys(table_name, start, rows);
		} catch (ServerException | IOException e) {
			throw ServerException.getJsonException(LOGGER, e);
		}
	}

	@Override
	public Boolean deleteRow(String table_name, String row_id) {
		try {
			return tableManager.deleteRow(table_name, row_id);
		} catch (ServerException | IOException e) {
			throw ServerException.getJsonException(LOGGER, e);
		}
	}

	@Override
	public TableRequestResult queryRows(String table_name, TableRequest request) {
		try {
			return tableManager.query(table_name, request);
		} catch (ServerException | IOException | Query.QueryException e) {
			throw ServerException.getJsonException(LOGGER, e);
		}
	}

}
