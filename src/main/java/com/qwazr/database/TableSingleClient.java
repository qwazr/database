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
import com.qwazr.utils.http.HttpResponseEntityException;
import com.qwazr.utils.http.HttpUtils;
import com.qwazr.utils.json.client.JsonClientAbstract;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableSingleClient extends JsonClientAbstract implements TableServiceInterface {

	public final static TypeReference<Set<String>> SetStringTypeRef = new TypeReference<Set<String>>() {
	};

	public final static TypeReference<Map<String, Object>> MapStringObjectTypeRef = new TypeReference<Map<String, Object>>() {
	};

	public TableSingleClient(String url, int msTimeOut) throws URISyntaxException {
		super(url, msTimeOut);
	}

	@Override
	public Set<String> list(Integer msTimeOut, Boolean local) {
		UBuilder uriBuilder = new UBuilder("/table").setParameterObject("local", local);
		Request request = Request.Get(uriBuilder.build());
		return commonServiceRequest(request, null, null, SetStringTypeRef, 200);
	}

	@Override
	public TableDefinition createTable(String table_name) {
		UBuilder uriBuilder = new UBuilder("/table/", table_name);
		Request request = Request.Post(uriBuilder.build());
		return commonServiceRequest(request, null, null, TableDefinition.class, 200);
	}

	@Override
	public TableDefinition getTable(String table_name) {
		UBuilder uriBuilder = new UBuilder("/table/", table_name);
		Request request = Request.Get(uriBuilder.build());
		return commonServiceRequest(request, null, null, TableDefinition.class, 200);
	}

	@Override
	public Boolean deleteTable(String table_name) {
		try {
			UBuilder uriBuilder = new UBuilder("/table/", table_name);
			Request request = Request.Delete(uriBuilder.build());
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
		UBuilder uriBuilder = new UBuilder("/table/", table_name, "/column");
		Request request = Request.Get(uriBuilder.build());
		return commonServiceRequest(request, null, null, ColumnDefinition.MapStringColumnTypeRef, 200);
	}

	@Override
	public ColumnDefinition getColumn(String table_name, String column_name) {
		UBuilder uriBuilder = new UBuilder("/table/", table_name, "/column/", column_name);
		Request request = Request.Get(uriBuilder.build());
		return commonServiceRequest(request, null, null, ColumnDefinition.class, 200);
	}

	@Override
	public ColumnDefinition addColumn(String table_name, String column_name, ColumnDefinition columnDefinition) {
		UBuilder uriBuilder = new UBuilder("/table/", table_name, "/column/", column_name);
		Request request = Request.Post(uriBuilder.build());
		return commonServiceRequest(request, columnDefinition, null, ColumnDefinition.class, 200);
	}

	@Override
	public Boolean removeColumn(String table_name, String column_name) {
		UBuilder uriBuilder = new UBuilder("/table/", table_name, "/column/", column_name);
		Request request = Request.Delete(uriBuilder.build());
		return commonServiceRequest(request, null, null, Boolean.class, 200);
	}

	@Override
	public Long upsertRows(String table_name, List<Map<String, Object>> rows) {
		UBuilder uriBuilder = new UBuilder("/table/", table_name, "/row");
		Request request = Request.Post(uriBuilder.build());
		return commonServiceRequest(request, rows, null, Long.class, 200);
	}

	@Override
	public Long upsertRows(String table_name, Integer buffer, InputStream inpustStream) {
		throw new WebApplicationException("Not yet implemented");
	}

	@Override
	public Map<String, Object> upsertRow(String table_name, String row_id, Map<String, Object> row) {
		UBuilder uriBuilder = new UBuilder("/table/", table_name, "/row/", row_id);
		Request request = Request.Put(uriBuilder.build());
		return commonServiceRequest(request, row, null, MapStringObjectTypeRef, 200);
	}

	@Override
	public Map<String, Object> getRow(String table_name, String row_id, Set<String> columns) {
		UBuilder uriBuilder = new UBuilder("/table/", table_name, "/row/", row_id);
		Request request = Request.Get(uriBuilder.build());
		return commonServiceRequest(request, columns, null, MapStringObjectTypeRef, 200);
	}

	@Override
	public Boolean deleteRow(String table_name, String row_id) {
		UBuilder uriBuilder = new UBuilder("/table/", table_name, "/row/", row_id);
		Request request = Request.Delete(uriBuilder.build());
		return commonServiceRequest(request, null, null, Boolean.class, 200);
	}

	@Override
	public TableRequestResult queryRows(String table_name, TableRequest tableRequest) {
		UBuilder uriBuilder = new UBuilder("/table/", table_name, "/query");
		Request request = Request.Post(uriBuilder.build());
		return commonServiceRequest(request, tableRequest, null, TableRequestResult.class, 200);
	}
}
