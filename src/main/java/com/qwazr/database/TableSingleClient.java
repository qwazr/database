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
import com.qwazr.utils.http.HttpRequest;
import com.qwazr.utils.http.HttpResponseEntityException;
import com.qwazr.utils.http.HttpUtils;
import com.qwazr.utils.json.client.JsonClientAbstract;
import com.qwazr.utils.server.RemoteService;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;

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

	public TableSingleClient(final RemoteService remote) {
		super(remote);
	}

	@Override
	public Set<String> list(final Integer msTimeOut, final Boolean local) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/table").setParameterObject("local", local);
		final HttpRequest request = HttpRequest.Get(uriBuilder.buildNoEx());
		return commonServiceRequest(request, null, null, SetStringTypeRef, 200);
	}

	@Override
	public TableDefinition createTable(final String table_name) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/table/", table_name);
		final HttpRequest request = HttpRequest.Post(uriBuilder.buildNoEx());
		return commonServiceRequest(request, null, null, TableDefinition.class, 200);
	}

	@Override
	public TableDefinition getTable(final String table_name) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/table/", table_name);
		final HttpRequest request = HttpRequest.Get(uriBuilder.buildNoEx());
		return commonServiceRequest(request, null, null, TableDefinition.class, 200);
	}

	@Override
	public Boolean deleteTable(final String table_name) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/table/", table_name);
		final HttpRequest request = HttpRequest.Delete(uriBuilder.buildNoEx());
		try (final CloseableHttpResponse response = execute(request, null, null)) {
			HttpUtils.checkStatusCodes(response, 200);
			return true;
		} catch (HttpResponseEntityException e) {
			throw e.getWebApplicationException();
		} catch (IOException e) {
			throw new WebApplicationException(e.getMessage(), e, Response.Status.INTERNAL_SERVER_ERROR);
		}
	}

	@Override
	public Map<String, ColumnDefinition> getColumns(final String table_name) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/table/", table_name, "/column");
		final HttpRequest request = HttpRequest.Get(uriBuilder.buildNoEx());
		return commonServiceRequest(request, null, null, ColumnDefinition.MapStringColumnTypeRef, 200);
	}

	@Override
	public ColumnDefinition getColumn(final String table_name, final String column_name) {
		final UBuilder uriBuilder =
				RemoteService.getNewUBuilder(remote, "/table/", table_name, "/column/", column_name);
		final HttpRequest request = HttpRequest.Get(uriBuilder.buildNoEx());
		return commonServiceRequest(request, null, null, ColumnDefinition.class, 200);
	}

	@Override
	public List<Object> getColumnTerms(final String table_name, final String column_name, final Integer start,
			final Integer rows) {
		final UBuilder uriBuilder =
				RemoteService.getNewUBuilder(remote, "/table/", table_name, "/column/", column_name, "/term")
						.setParameter("start", start).setParameter("rows", rows);
		final HttpRequest request = HttpRequest.Get(uriBuilder.buildNoEx());
		return commonServiceRequest(request, null, null, ListObjectTypeRef, 200);
	}

	@Override
	public List<String> getColumnTermKeys(final String table_name, final String column_name, final String term,
			final Integer start,
			Integer rows) {
		final UBuilder uriBuilder =
				RemoteService.getNewUBuilder(remote, "/table/", table_name, "/column/", column_name, "/term/", term).
						setParameter("start", start).setParameter("rows", rows);
		final HttpRequest request = HttpRequest.Get(uriBuilder.buildNoEx());
		return commonServiceRequest(request, null, null, ListStringTypeRef, 200);
	}

	@Override
	public ColumnDefinition setColumn(final String table_name, final String column_name,
			final ColumnDefinition columnDefinition) {
		final UBuilder uriBuilder =
				RemoteService.getNewUBuilder(remote, "/table/", table_name, "/column/", column_name);
		final HttpRequest request = HttpRequest.Post(uriBuilder.buildNoEx());
		return commonServiceRequest(request, columnDefinition, null, ColumnDefinition.class, 200);
	}

	@Override
	public Boolean removeColumn(final String table_name, final String column_name) {
		final UBuilder uriBuilder =
				RemoteService.getNewUBuilder(remote, "/table/", table_name, "/column/", column_name);
		final HttpRequest request = HttpRequest.Delete(uriBuilder.buildNoEx());
		return commonServiceRequest(request, null, null, Boolean.class, 200);
	}

	@Override
	public Long upsertRows(final String table_name, final List<Map<String, Object>> rows) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/table/", table_name, "/row");
		final HttpRequest request = HttpRequest.Post(uriBuilder.buildNoEx());
		return commonServiceRequest(request, rows, null, Long.class, 200);
	}

	@Override
	public Long upsertRows(final String table_name, final Integer buffer, final InputStream inpustStream) {
		throw new WebApplicationException("Not yet implemented");
	}

	@Override
	public Map<String, Object> upsertRow(final String table_name, final String row_id, final Map<String, Object> row) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/table/", table_name, "/row/", row_id);
		final HttpRequest request = HttpRequest.Put(uriBuilder.buildNoEx());
		return commonServiceRequest(request, row, null, MapStringObjectTypeRef, 200);
	}

	@Override
	public Map<String, Object> getRow(final String table_name, final String row_id, final Set<String> columns) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/table/", table_name, "/row/", row_id);
		if (columns != null)
			for (String column : columns)
				uriBuilder.addParameter("column", column);
		final HttpRequest request = HttpRequest.Get(uriBuilder.buildNoEx());
		return commonServiceRequest(request, null, null, MapStringObjectTypeRef, 200);
	}

	@Override
	public List<String> getRows(final String table_name, final Integer start, final Integer rows) {
		final UBuilder uriBuilder =
				RemoteService.getNewUBuilder(remote, "/table/", table_name, "/row").setParameter("start", start)
						.setParameter("rows", rows);
		final HttpRequest request = HttpRequest.Get(uriBuilder.buildNoEx());
		return commonServiceRequest(request, null, null, ListStringTypeRef, 200);
	}

	@Override
	public Boolean deleteRow(final String table_name, final String row_id) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/table/", table_name, "/row/", row_id);
		final HttpRequest request = HttpRequest.Delete(uriBuilder.buildNoEx());
		return commonServiceRequest(request, null, null, Boolean.class, 200);
	}

	@Override
	public TableRequestResult queryRows(final String table_name, final TableRequest tableRequest) {
		final UBuilder uriBuilder = RemoteService.getNewUBuilder(remote, "/table/", table_name, "/query");
		final HttpRequest request = HttpRequest.Post(uriBuilder.buildNoEx());
		return commonServiceRequest(request, tableRequest, null, TableRequestResult.class, 200);
	}
}
