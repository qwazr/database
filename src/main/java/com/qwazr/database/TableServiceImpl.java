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

import com.fasterxml.jackson.core.type.TypeReference;
import com.qwazr.database.model.ColumnDefinition;
import com.qwazr.database.model.TableDefinition;
import com.qwazr.database.model.TableRequest;
import com.qwazr.database.model.TableRequestResult;
import com.qwazr.database.store.KeyStore;
import com.qwazr.database.store.Query;
import com.qwazr.utils.json.JsonMapper;
import com.qwazr.utils.server.ServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

public class TableServiceImpl implements TableServiceInterface {

	private static final Logger LOGGER = LoggerFactory.getLogger(TableServiceImpl.class);

	@Override
	public Set<String> list(Integer msTimeOut, Boolean local) {
		return TableManager.INSTANCE.getNameSet();
	}

	@Override
	public TableDefinition createTable(final String tableName, KeyStore.Impl storeImplementation) {
		try {
			if (storeImplementation == null)
				storeImplementation = KeyStore.Impl.leveldb;
			TableManager.INSTANCE.createTable(tableName, storeImplementation);
			return new TableDefinition(storeImplementation, TableManager.INSTANCE.getColumns(tableName));
		} catch (Exception e) {
			throw ServerException.getJsonException(LOGGER, e);
		}
	}

	@Override
	public TableDefinition getTable(String tableName) {
		try {
			return new TableDefinition(null, TableManager.INSTANCE.getColumns(tableName));
		} catch (IOException | ServerException e) {
			throw ServerException.getJsonException(LOGGER, e);
		}
	}

	@Override
	public Boolean deleteTable(String tableName) {
		try {
			TableManager.INSTANCE.deleteTable(tableName);
			return true;
		} catch (IOException | ServerException e) {
			throw ServerException.getJsonException(LOGGER, e);
		}
	}

	@Override
	public Map<String, ColumnDefinition> getColumns(String tableName) {
		try {
			return TableManager.INSTANCE.getColumns(tableName);
		} catch (ServerException | IOException e) {
			throw ServerException.getJsonException(LOGGER, e);
		}
	}

	@Override
	public ColumnDefinition getColumn(final String tableName, final String columnName) {
		try {
			return TableManager.INSTANCE.getColumns(tableName).get(columnName);
		} catch (ServerException | IOException e) {
			throw ServerException.getJsonException(LOGGER, e);
		}
	}

	@Override
	public List<Object> getColumnTerms(final String tableName, final String columnName, final Integer start,
			final Integer rows) {
		try {
			return TableManager.INSTANCE.getColumnTerms(tableName, columnName, start, rows);
		} catch (ServerException | IOException e) {
			throw ServerException.getJsonException(LOGGER, e);
		}
	}

	@Override
	public List<String> getColumnTermKeys(final String tableName, final String columnName, final String term,
			final Integer start, final Integer rows) {
		try {
			return TableManager.INSTANCE.getColumnTermKeys(tableName, columnName, term, start, rows);
		} catch (ServerException | IOException e) {
			throw ServerException.getJsonException(LOGGER, e);
		}
	}

	@Override
	public ColumnDefinition setColumn(String tableName, String columnName, ColumnDefinition columnDefinition) {
		try {
			TableManager.INSTANCE.setColumn(tableName, columnName, columnDefinition);
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
			return (long) TableManager.INSTANCE.upsertRows(table_name, rows);
		} catch (IOException | ServerException e) {
			throw ServerException.getJsonException(LOGGER, e);
		}
	}

	public final static TypeReference<Map<String, Object>> MapStringColumnValueTypeRef =
			new TypeReference<Map<String, Object>>() {
			};

	private final int flushBuffer(String table_name, List<Map<String, Object>> buffer)
			throws IOException, ServerException {
		try {
			if (buffer == null || buffer.isEmpty())
				return 0;
			TableManager.INSTANCE.upsertRows(table_name, buffer);
			return buffer.size();
		} finally {
			buffer.clear();
		}
	}

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
			int res = TableManager.INSTANCE.upsertRows(tableName, buffer);
			buffer.clear();
			return res;
		}

	}

	@Override
	public Long upsertRows(String table_name, Integer bufferSize, InputStream inputStream) {
		if (bufferSize == null || bufferSize < 1)
			bufferSize = 50;

		try (final InputStreamReader irs = new InputStreamReader(inputStream, "UTF-8");
				final BufferedReader br = new BufferedReader(irs)) {
			long counter = 0;
			String line;
			BufferFlush buffer = new BufferFlush(bufferSize, table_name);
			while ((line = br.readLine()) != null) {
				line = line.trim();
				if (line.isEmpty())
					continue;
				Map<String, Object> nodeMap = JsonMapper.MAPPER.readValue(line, MapStringColumnValueTypeRef);
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
			TableManager.INSTANCE.upsertRow(table_name, row_id, node);
			return node;
		} catch (ServerException | IOException e) {
			throw ServerException.getJsonException(LOGGER, e);
		}
	}

	@Override
	public LinkedHashMap<String, Object> getRow(String table_name, String row_id, Set<String> columns) {
		try {
			return TableManager.INSTANCE.getRow(table_name, row_id, columns);
		} catch (ServerException | IOException e) {
			throw ServerException.getJsonException(LOGGER, e);
		}
	}

	@Override
	public List<String> getRows(String table_name, Integer start, Integer rows) {
		try {
			return TableManager.INSTANCE.getPrimaryKeys(table_name, start, rows);
		} catch (ServerException | IOException e) {
			throw ServerException.getJsonException(LOGGER, e);
		}
	}

	@Override
	public Boolean deleteRow(String table_name, String row_id) {
		try {
			return TableManager.INSTANCE.deleteRow(table_name, row_id);
		} catch (ServerException | IOException e) {
			throw ServerException.getJsonException(LOGGER, e);
		}
	}

	@Override
	public TableRequestResult queryRows(String table_name, TableRequest request) {
		try {
			return TableManager.INSTANCE.query(table_name, request);
		} catch (ServerException | IOException | Query.QueryException e) {
			throw ServerException.getJsonException(LOGGER, e);
		}
	}

}
