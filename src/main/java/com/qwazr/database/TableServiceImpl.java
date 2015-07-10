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
import com.qwazr.database.model.TableRequest;
import com.qwazr.utils.server.ServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

public class TableServiceImpl implements TableServiceInterface {

	private static final Logger logger = LoggerFactory
			.getLogger(TableServiceImpl.class);


	@Override
	public Set<String> list(Integer msTimeOut, Boolean local) {

		return TableManager.INSTANCE.nameSet();

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

	private TableDefinition getTableOrNotFound(String tableName)
			throws ServerException {
		TableDefinition tableDef = TableManager.INSTANCE.get(tableName);
		if (tableDef == null)
			throw new ServerException(Status.NOT_FOUND, "Table not found: "
					+ tableName);
		return tableDef;
	}

	@Override
	public TableDefinition getTable(String tableName, Integer msTimeOut,
									Boolean local) {
		return TableManager.INSTANCE
				.get(tableName);
	}

	private TableDefinition deleteTableLocal(String tableName)
			throws IOException, URISyntaxException, ServerException {
		TableDefinition tableDefinition = getTableOrNotFound(tableName);
		TableManager.INSTANCE.delete(tableName);
		return tableDefinition;
	}

	@Override
	public TableDefinition deleteTable(String tableName, Integer msTimeOut,
									   Boolean local) {
		try {
			return deleteTableLocal(tableName);
		} catch (Exception e) {
			logger.warn(e.getMessage(), e);
			throw ServerException.getJsonException(e);
		}
	}

	@Override
	public Long upsertRows(@PathParam("table_name") String table_name, LinkedHashMap<String, Object> rows) {
		return null;
	}

	@Override
	public Long upsertRows(@PathParam("table_name") String table_name, InputStream inpustStream) {
		return null;
	}

	@Override
	public LinkedHashMap<String, Object> upsertRow(@PathParam("table_name") String table_name, @PathParam("row_id") String row_id, LinkedHashMap<String, Object> node) {
		return null;
	}

	@Override
	public LinkedHashMap<String, Object> getRow(@PathParam("table_name") String table_name, @PathParam("node_id") String row_id) {
		return null;
	}

	@Override
	public LinkedHashMap<String, Object> deleteRow(@PathParam("table_name") String table_name, @PathParam("row_id") String row_id) {
		return null;
	}

	@Override
	public List<LinkedHashMap<String, Object>> requestNodes(@PathParam("table_name") String graph_name, TableRequest request) {
		return null;
	}


}
