/*
 * Copyright 2016-2017 Emmanuel Keller / QWAZR
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
import com.qwazr.database.model.TableDefinition;
import com.qwazr.database.model.TableRequest;
import com.qwazr.database.model.TableRequestResult;
import com.qwazr.database.model.TableStatus;
import com.qwazr.database.store.KeyStore;
import com.qwazr.server.RemoteService;
import com.qwazr.server.client.JsonClient;
import com.qwazr.utils.StringUtils;

import javax.ws.rs.NotAcceptableException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

public class TableSingleClient extends JsonClient implements TableServiceInterface {

	private final WebTarget tableTarget;

	public TableSingleClient(final RemoteService remote) {
		super(remote);
		tableTarget = client.target(remote.serviceAddress).path("table");
	}

	@Override
	public SortedSet<String> list() {
		return tableTarget.request(MediaType.APPLICATION_JSON).get(sortedSetStringType);
	}

	@Override
	public TableDefinition createTable(final String tableName, final KeyStore.Impl storeImplementation) {
		WebTarget target = tableTarget.path(tableName);
		if (storeImplementation != null)
			target = target.queryParam("implementation", storeImplementation);
		return target.request(MediaType.APPLICATION_JSON).post(Entity.json(null), TableDefinition.class);
	}

	@Override
	public TableStatus getTableStatus(final String tableName) {
		return tableTarget.path(tableName).request(MediaType.APPLICATION_JSON).get(TableStatus.class);
	}

	@Override
	public Boolean deleteTable(final String tableName) {
		return tableTarget.path(tableName).request(MediaType.APPLICATION_JSON).delete(Boolean.class);
	}

	@Override
	public Map<String, ColumnDefinition> getColumns(final String tableName) {
		return tableTarget.path(tableName)
				.path("column")
				.request(MediaType.APPLICATION_JSON)
				.get(ColumnDefinition.mapStringColumnType);
	}

	@Override
	public ColumnDefinition getColumn(final String tableName, final String columnName) {
		return tableTarget.path(tableName)
				.path("column")
				.path(columnName)
				.request(MediaType.APPLICATION_JSON)
				.get(ColumnDefinition.class);
	}

	@Override
	public List<Object> getColumnTerms(final String tableName, final String columnName, final Integer start,
			final Integer rows) {
		WebTarget target = tableTarget.path(tableName).path("column").path(columnName).path("term");
		if (start != null)
			target = target.queryParam("start", start);
		if (rows != null)
			target = target.queryParam("rows", rows);
		return target.request(MediaType.APPLICATION_JSON).get(listObjectType);
	}

	@Override
	public List<String> getColumnTermKeys(final String tableName, final String columnName, final String term,
			final Integer start, Integer rows) {
		return tableTarget.path(tableName)
				.path("column")
				.path(columnName)
				.path("term")
				.path(term)
				.request(MediaType.APPLICATION_JSON)
				.get(listStringType);
	}

	@Override
	public ColumnDefinition setColumn(final String tableName, final String columnName,
			final ColumnDefinition columnDefinition) {
		return tableTarget.path(tableName)
				.path("column")
				.path(columnName)
				.request(MediaType.APPLICATION_JSON)
				.post(Entity.json(columnDefinition), ColumnDefinition.class);
	}

	@Override
	public Boolean removeColumn(final String tableName, final String columnName) {
		return tableTarget.path(tableName)
				.path("column")
				.path(columnName)
				.request(MediaType.APPLICATION_JSON)
				.delete(Boolean.class);
	}

	@Override
	public Long upsertRows(final String tableName, final List<Map<String, Object>> rows) {
		return tableTarget.path(tableName)
				.path("row")
				.request(MediaType.APPLICATION_JSON)
				.post(Entity.json(rows), Long.class);
	}

	@Override
	public Long upsertRows(final String tableName, final Integer buffer, final InputStream inputStream) {
		WebTarget target = tableTarget.path(tableName).path("row");
		if (buffer != null)
			target = target.queryParam("buffer", buffer);
		return target.request(MediaType.APPLICATION_JSON)
				.post(Entity.entity(inputStream, MediaType.TEXT_PLAIN), Long.class);
	}

	@Override
	public Map<String, Object> upsertRow(final String tableName, final String rowId, final Map<String, Object> row) {
		return tableTarget.path(tableName)
				.path("row")
				.path(rowId)
				.request(MediaType.APPLICATION_JSON)
				.put(Entity.json(row), mapStringObjectType);
	}

	@Override
	public Map<String, Object> getRow(final String tableName, final String rowId, final Set<String> columns) {
		WebTarget target = tableTarget.path(tableName).path("row").path(rowId);
		if (columns != null)
			target = target.queryParam("column", columns.toArray());
		return target.request(MediaType.APPLICATION_JSON).get(mapStringObjectType);
	}

	@Override
	public List<Map<String, Object>> getRows(final String tableName, final Set<String> columns,
			final Set<String> rowsIds) {
		WebTarget target = tableTarget.path(tableName).path("rows");
		if (columns != null)
			target = target.queryParam("column", columns.toArray());
		return target.request(MediaType.APPLICATION_JSON).post(Entity.json(rowsIds), listMapStringObjectType);
	}

	@Override
	public List<String> getRows(final String tableName, final Integer start, final Integer rows) {
		WebTarget target = tableTarget.path(tableName).path("row");
		if (start != null)
			target = target.queryParam("start", start);
		if (rows != null)
			target = target.queryParam("rows", rows);
		return target.request(MediaType.APPLICATION_JSON).get(listStringType);
	}

	@Override
	public Boolean deleteRow(final String tableName, final String rowId) {
		if (StringUtils.isEmpty(rowId))
			throw new NotAcceptableException("The key is missing");
		return tableTarget.path(tableName)
				.path("row")
				.path(rowId)
				.request(MediaType.APPLICATION_JSON)
				.delete(Boolean.class);
	}

	@Override
	public TableRequestResult queryRows(final String tableName, final TableRequest tableRequest) {
		return tableTarget.path(tableName)
				.path("query")
				.request(MediaType.APPLICATION_JSON)
				.post(Entity.json(tableRequest), TableRequestResult.class);
	}
}
