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

import com.fasterxml.jackson.core.type.TypeReference;
import com.qwazr.database.model.TableDefinition;
import com.qwazr.database.model.TableRequest;
import com.qwazr.database.model.TableRequestResult;
import com.qwazr.database.store.DatabaseException;
import com.qwazr.database.store.Query;
import com.qwazr.utils.json.JsonMapper;
import com.qwazr.utils.server.ServerException;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

public class TableServiceImpl implements TableServiceInterface {

	private static final Logger logger = LoggerFactory
			.getLogger(TableServiceImpl.class);


	@Override
	public Set<String> list(Integer msTimeOut, Boolean local) {
		return TableManager.INSTANCE.getNameSet();
	}

	@Override
	public TableDefinition createUpdateTable(String tableName,
											 TableDefinition tableDef, Integer msTimeOut, Boolean local) {
		try {
			TableManager.INSTANCE.createUpdateTable(tableName, tableDef);
			return tableDef;
		} catch (Exception e) {
			logger.warn(e.getMessage(), e);
			throw ServerException.getJsonException(e);
		}
	}

	@Override
	public TableDefinition getTable(String tableName, Integer msTimeOut,
									Boolean local) {
		try {
			return TableManager.INSTANCE.getTableDefinition(tableName);
		} catch (IOException | ServerException | DatabaseException e) {
			logger.warn(e.getMessage(), e);
			throw ServerException.getJsonException(e);
		}
	}

	@Override
	public Boolean deleteTable(String tableName, Integer msTimeOut,
							   Boolean local) {
		try {
			TableManager.INSTANCE.delete(tableName);
			return true;
		} catch (IOException | ServerException e) {
			logger.warn(e.getMessage(), e);
			throw ServerException.getJsonException(e);
		}
	}

	@Override
	public Long upsertRows(String table_name, LinkedHashMap<String, Object> rows) {
		return null;
	}

	public final static TypeReference<Map<String, Object>> MapStringColumnValueTypeRef =
			new TypeReference<Map<String, Object>>() {
			};

	private final int flushBuffer(String table_name, List<Map<String, Object>> buffer)
			throws IOException, ServerException, DatabaseException {
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
			this.buffer = new ArrayList<Map<String, Object>>(bufferSize);
			this.tableName = tableName;
		}

		private int addRow(Map<String, Object> row) {
			buffer.add(row);
			return buffer.size();
		}

		private int flush() throws ServerException, DatabaseException, IOException {
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
		
		try {
			InputStreamReader irs = null;
			BufferedReader br = null;
			try {
				irs = new InputStreamReader(inputStream, "UTF-8");
				br = new BufferedReader(irs);
				long counter = 0;
				String line;
				BufferFlush buffer = new BufferFlush(bufferSize, table_name);
				while ((line = br.readLine()) != null) {
					line = line.trim();
					if (line.isEmpty())
						continue;
					Map<String, Object> nodeMap = JsonMapper.MAPPER
							.readValue(line, MapStringColumnValueTypeRef);
					if (buffer.addRow(nodeMap) == bufferSize)
						counter += buffer.flush();
				}
				counter += buffer.flush();
				return counter;
			} finally {
				if (br != null)
					IOUtils.closeQuietly(br);
				if (irs != null)
					IOUtils.closeQuietly(irs);
			}
		} catch (Exception e) {
			logger.warn(e.getMessage(), e);
			throw ServerException.getJsonException(e);
		}
	}

	@Override
	public LinkedHashMap<String, Object> upsertRow(String table_name,
												   String row_id,
												   LinkedHashMap<String, Object> node) {
		try {
			TableManager.INSTANCE.upsertRow(table_name, row_id, node);
			return node;
		} catch (ServerException | IOException | DatabaseException e) {
			logger.warn(e.getMessage(), e);
			throw ServerException.getJsonException(e);
		}
	}

	@Override
	public LinkedHashMap<String, Object> getRow(String table_name, String row_id, Set<String> columns) {
		try {
			return TableManager.INSTANCE.getRow(table_name, row_id, columns);
		} catch (ServerException | IOException | DatabaseException e) {
			logger.warn(e.getMessage(), e);
			throw ServerException.getJsonException(e);
		}
	}

	@Override
	public Boolean deleteRow(String table_name, String row_id) {
		try {
			return TableManager.INSTANCE.deleteRow(table_name, row_id);
		} catch (ServerException | IOException | DatabaseException e) {
			logger.warn(e.getMessage(), e);
			throw ServerException.getJsonException(e);
		}
	}

	@Override
	public TableRequestResult queryRows(String table_name, TableRequest request) {
		try {
			return TableManager.INSTANCE.query(table_name, request);
		} catch (ServerException | IOException | Query.QueryException | DatabaseException e) {
			logger.warn(e.getMessage(), e);
			throw ServerException.getJsonException(e);
		}
	}


}
