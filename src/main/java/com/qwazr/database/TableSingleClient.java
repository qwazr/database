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
import com.qwazr.utils.UBuilder;
import com.qwazr.utils.http.HttpResponseEntityException;
import com.qwazr.utils.http.HttpUtils;
import com.qwazr.utils.json.client.JsonClientAbstract;
import com.qwazr.utils.server.RemoteService;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.io.IOException;
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

	public TableSingleClient(RemoteService remote) {
		super(remote);
	}

	@Override
	public Set<String> list(Integer msTimeOut, Boolean local) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/table").setParameterObject("local", local);
		Request request = Request.Get(uriBuilder.buildNoEx());
		return commonServiceRequest(request, null, null, SetStringTypeRef, 200);
	}

	@Override
	public TableDefinition createTable(String table_name) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/table/", table_name);
		Request request = Request.Post(uriBuilder.buildNoEx());
		return commonServiceRequest(request, null, null, TableDefinition.class, 200);
	}

	@Override
	public TableDefinition getTable(String table_name) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/table/", table_name);
		Request request = Request.Get(uriBuilder.buildNoEx());
		return commonServiceRequest(request, null, null, TableDefinition.class, 200);
	}

	@Override
	public Boolean deleteTable(String table_name) {
		try {
			final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/table/", table_name);
			Request request = Request.Delete(uriBuilder.buildNoEx());
			HttpResponse response = execute(request, null, null);
			HttpUtils.checkStatusCodes(response, 200);
			return true;
		} catch (HttpResponseEntityException e) {
			throw e.getWebApplicationException();
		} catch (IOException e) {
			throw new WebApplicationException(e.getMessage(), e, Response.Status.INTERNAL_SERVER_ERROR);
		}
	}

	@Override
	public Map<String, ColumnDefinition> getColumns(String table_name) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/table/", table_name, "/column");
		Request request = Request.Get(uriBuilder.buildNoEx());
		return commonServiceRequest(request, null, null, ColumnDefinition.MapStringColumnTypeRef, 200);
	}

	@Override
	public ColumnDefinition getColumn(String table_name, String column_name) {
		final UBuilder uriBuilder =
				RemoteService.getNewUBuilder(remote, "/table/", table_name, "/column/", column_name);
		Request request = Request.Get(uriBuilder.buildNoEx());
		return commonServiceRequest(request, null, null, ColumnDefinition.class, 200);
	}

	@Override
	public List<Object> getColumnTerms(String table_name, String column_name, Integer start, Integer rows) {
		final UBuilder uriBuilder =
				RemoteService.getNewUBuilder(remote, "/table/", table_name, "/column/", column_name, "/term")
						.setParameter("start", start).setParameter("rows", rows);
		Request request = Request.Get(uriBuilder.buildNoEx());
		return commonServiceRequest(request, null, null, ListObjectTypeRef, 200);
	}

	@Override
	public List<String> getColumnTermKeys(String table_name, String column_name, String term, Integer start,
			Integer rows) {
		final UBuilder uriBuilder =
				RemoteService.getNewUBuilder(remote, "/table/", table_name, "/column/", column_name, "/term/", term).
						setParameter("start", start).setParameter("rows", rows);
		Request request = Request.Get(uriBuilder.buildNoEx());
		return commonServiceRequest(request, null, null, ListStringTypeRef, 200);
	}

	@Override
	public ColumnDefinition setColumn(String table_name, String column_name, ColumnDefinition columnDefinition) {
		final UBuilder uriBuilder =
				RemoteService.getNewUBuilder(remote, "/table/", table_name, "/column/", column_name);
		Request request = Request.Post(uriBuilder.buildNoEx());
		return commonServiceRequest(request, columnDefinition, null, ColumnDefinition.class, 200);
	}

	@Override
	public Boolean removeColumn(String table_name, String column_name) {
		final UBuilder uriBuilder =
				RemoteService.getNewUBuilder(remote, "/table/", table_name, "/column/", column_name);
		Request request = Request.Delete(uriBuilder.buildNoEx());
		return commonServiceRequest(request, null, null, Boolean.class, 200);
	}

	@Override
	public Long upsertRows(String table_name, List<Map<String, Object>> rows) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/table/", table_name, "/row");
		Request request = Request.Post(uriBuilder.buildNoEx());
		return commonServiceRequest(request, rows, null, Long.class, 200);
	}

	@Override
	public Long upsertRows(String table_name, Integer buffer, InputStream inpustStream) {
		throw new WebApplicationException("Not yet implemented");
	}

	@Override
	public Map<String, Object> upsertRow(String table_name, String row_id, Map<String, Object> row) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/table/", table_name, "/row/", row_id);
		Request request = Request.Put(uriBuilder.buildNoEx());
		return commonServiceRequest(request, row, null, MapStringObjectTypeRef, 200);
	}

	@Override
	public Map<String, Object> getRow(String table_name, String row_id, Set<String> columns) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/table/", table_name, "/row/", row_id);
		if (columns != null)
			for (String column : columns)
				uriBuilder.addParameter("column", column);
		Request request = Request.Get(uriBuilder.buildNoEx());
		return commonServiceRequest(request, null, null, MapStringObjectTypeRef, 200);
	}

	@Override
	public List<String> getRows(String table_name, Integer start, Integer rows) {
		final UBuilder uriBuilder =
				RemoteService.getNewUBuilder(remote, "/table/", table_name, "/row").setParameter("start", start)
						.setParameter("rows", rows);
		Request request = Request.Get(uriBuilder.buildNoEx());
		return commonServiceRequest(request, null, null, ListStringTypeRef, 200);
	}

	@Override
	public Boolean deleteRow(String table_name, String row_id) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/table/", table_name, "/row/", row_id);
		Request request = Request.Delete(uriBuilder.buildNoEx());
		return commonServiceRequest(request, null, null, Boolean.class, 200);
	}

	@Override
	public TableRequestResult queryRows(String table_name, TableRequest tableRequest) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/table/", table_name, "/query");
		Request request = Request.Post(uriBuilder.buildNoEx());
		return commonServiceRequest(request, tableRequest, null, TableRequestResult.class, 200);
	}
}
