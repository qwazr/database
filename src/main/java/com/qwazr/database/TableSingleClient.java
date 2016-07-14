/**
 * Copyright 2016 Emmanuel Keller / QWAZR
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
import com.qwazr.utils.UBuilder;
import com.qwazr.utils.http.HttpRequest;
import com.qwazr.utils.json.client.JsonClientAbstract;
import com.qwazr.utils.server.RemoteService;

import javax.ws.rs.WebApplicationException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableSingleClient extends JsonClientAbstract implements TableServiceInterface {

	public final static TypeReference<Set<String>> SetStringTypeRef = new TypeReference<Set<String>>() {
	};

	public final static TypeReference<List<Object>> ListObjectTypeRef = new TypeReference<List<Object>>() {
	};

	public final static TypeReference<List<String>> ListStringTypeRef = new TypeReference<List<String>>() {
	};

	public final static TypeReference<Map<String, Object>> MapStringObjectTypeRef =
			new TypeReference<Map<String, Object>>() {
			};

	public final static TypeReference<List<Map<String, Object>>> ListMapStringObjectTypeRef =
			new TypeReference<List<Map<String, Object>>>() {
			};

	public TableSingleClient(final RemoteService remote) {
		super(remote);
	}

	@Override
	public Set<String> list(final Integer msTimeOut, final Boolean local) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/table").setParameterObject("local", local);
		final HttpRequest request = HttpRequest.Get(uriBuilder.buildNoEx());
		return executeJson(request, null, null, SetStringTypeRef, valid200Json);
	}

	@Override
	public TableDefinition createTable(final String tableName, final KeyStore.Impl storeImplementation) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/table/", tableName)
				.setParameter("implementation", storeImplementation);
		final HttpRequest request = HttpRequest.Post(uriBuilder.buildNoEx());
		return executeJson(request, null, null, TableDefinition.class, valid200Json);
	}

	@Override
	public TableDefinition getTable(final String tableName) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/table/", tableName);
		final HttpRequest request = HttpRequest.Get(uriBuilder.buildNoEx());
		return executeJson(request, null, null, TableDefinition.class, valid200Json);
	}

	@Override
	public Boolean deleteTable(final String tableName) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/table/", tableName);
		final HttpRequest request = HttpRequest.Delete(uriBuilder.buildNoEx());
		return executeStatusCode(request, null, null, valid200) == 200;
	}

	@Override
	public Map<String, ColumnDefinition> getColumns(final String tableName) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/table/", tableName, "/column");
		final HttpRequest request = HttpRequest.Get(uriBuilder.buildNoEx());
		return executeJson(request, null, null, ColumnDefinition.MapStringColumnTypeRef, valid200Json);
	}

	@Override
	public ColumnDefinition getColumn(final String tableName, final String columnName) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/table/", tableName, "/column/", columnName);
		final HttpRequest request = HttpRequest.Get(uriBuilder.buildNoEx());
		return executeJson(request, null, null, ColumnDefinition.class, valid200Json);
	}

	@Override
	public List<Object> getColumnTerms(final String tableName, final String columnName, final Integer start,
			final Integer rows) {
		final UBuilder uriBuilder =
				RemoteService.getNewUBuilder(remote, "/table/", tableName, "/column/", columnName, "/term")
						.setParameter("start", start)
						.setParameter("rows", rows);
		final HttpRequest request = HttpRequest.Get(uriBuilder.buildNoEx());
		return executeJson(request, null, null, ListObjectTypeRef, valid200Json);
	}

	@Override
	public List<String> getColumnTermKeys(final String tableName, final String columnName, final String term,
			final Integer start, Integer rows) {
		final UBuilder uriBuilder =
				RemoteService.getNewUBuilder(remote, "/table/", tableName, "/column/", columnName, "/term/", term).
						setParameter("start", start).setParameter("rows", rows);
		final HttpRequest request = HttpRequest.Get(uriBuilder.buildNoEx());
		return executeJson(request, null, null, ListStringTypeRef, valid200Json);
	}

	@Override
	public ColumnDefinition setColumn(final String tableName, final String columnName,
			final ColumnDefinition columnDefinition) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/table/", tableName, "/column/", columnName);
		final HttpRequest request = HttpRequest.Post(uriBuilder.buildNoEx());
		return executeJson(request, columnDefinition, null, ColumnDefinition.class, valid200Json);
	}

	@Override
	public Boolean removeColumn(final String tableName, final String columnName) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/table/", tableName, "/column/", columnName);
		final HttpRequest request = HttpRequest.Delete(uriBuilder.buildNoEx());
		return executeJson(request, null, null, Boolean.class, valid200Json);
	}

	@Override
	public Long upsertRows(final String tableName, final List<Map<String, Object>> rows) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/table/", tableName, "/row");
		final HttpRequest request = HttpRequest.Post(uriBuilder.buildNoEx());
		return executeJson(request, rows, null, Long.class, valid200Json);
	}

	@Override
	public Long upsertRows(final String tableName, final Integer buffer, final InputStream inputStream) {
		throw new WebApplicationException("Not yet implemented");
	}

	@Override
	public Map<String, Object> upsertRow(final String tableName, final String rowId, final Map<String, Object> row) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/table/", tableName, "/row/", rowId);
		final HttpRequest request = HttpRequest.Put(uriBuilder.buildNoEx());
		return executeJson(request, row, null, MapStringObjectTypeRef, valid200Json);
	}

	@Override
	public Map<String, Object> getRow(final String tableName, final String rowId, final Set<String> columns) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/table/", tableName, "/row/", rowId);
		if (columns != null)
			for (String column : columns)
				uriBuilder.addParameter("column", column);
		final HttpRequest request = HttpRequest.Get(uriBuilder.buildNoEx());
		return executeJson(request, null, null, MapStringObjectTypeRef, valid200Json);
	}

	@Override
	public List<String> getRows(final String tableName, final Integer start, final Integer rows) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/table/", tableName, "/row")
				.setParameter("start", start)
				.setParameter("rows", rows);
		final HttpRequest request = HttpRequest.Get(uriBuilder.buildNoEx());
		return executeJson(request, null, null, ListStringTypeRef, valid200Json);
	}

	@Override
	public Boolean deleteRow(final String tableName, final String rowId) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/table/", tableName, "/row/", rowId);
		final HttpRequest request = HttpRequest.Delete(uriBuilder.buildNoEx());
		return executeJson(request, null, null, Boolean.class, valid200Json);
	}

	@Override
	public TableRequestResult queryRows(final String tableName, final TableRequest tableRequest) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/table/", tableName, "/query");
		final HttpRequest request = HttpRequest.Post(uriBuilder.buildNoEx());
		return executeJson(request, tableRequest, null, TableRequestResult.class, valid200Json);
	}
}
