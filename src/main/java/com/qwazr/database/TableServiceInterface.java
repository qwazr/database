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

import com.qwazr.database.model.ColumnDefinition;
import com.qwazr.database.model.TableDefinition;
import com.qwazr.database.model.TableRequest;
import com.qwazr.database.model.TableRequestResult;
import com.qwazr.database.model.TableStatus;
import com.qwazr.database.store.KeyStore;
import com.qwazr.server.ServiceInterface;

import javax.annotation.security.RolesAllowed;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

@RolesAllowed(TableServiceInterface.SERVICE_NAME)
@Path("/" + TableServiceInterface.SERVICE_NAME)
public interface TableServiceInterface extends ServiceInterface {

	String SERVICE_NAME = "table";

	@GET
	@Path("/")
	@Produces(ServiceInterface.APPLICATION_JSON_UTF8)
	SortedSet<String> list();

	@POST
	@Path("/{table_name}")
	@Consumes(ServiceInterface.APPLICATION_JSON_UTF8)
	@Produces(ServiceInterface.APPLICATION_JSON_UTF8)
	TableDefinition createTable(@PathParam("table_name") String table_name,
			@QueryParam("implementation") KeyStore.Impl storeImplementation);

	@GET
	@Path("/{table_name}")
	@Produces(ServiceInterface.APPLICATION_JSON_UTF8)
	TableStatus getTableStatus(@PathParam("table_name") String table_name);

	@DELETE
	@Path("/{table_name}")
	@Produces(ServiceInterface.APPLICATION_JSON_UTF8)
	Boolean deleteTable(@PathParam("table_name") String table_name);

	@GET
	@Path("/{table_name}/column")
	@Produces(ServiceInterface.APPLICATION_JSON_UTF8)
	Map<String, ColumnDefinition> getColumns(@PathParam("table_name") String table_name);

	@GET
	@Path("/{table_name}/column/{column_name}")
	@Produces(ServiceInterface.APPLICATION_JSON_UTF8)
	ColumnDefinition getColumn(@PathParam("table_name") String table_name,
			@PathParam("column_name") String column_name);

	@GET
	@Path("/{table_name}/column/{column_name}/term")
	@Produces(ServiceInterface.APPLICATION_JSON_UTF8)
	List<Object> getColumnTerms(@PathParam("table_name") String table_name,
			@PathParam("column_name") String column_name, @QueryParam("start") Integer start,
			@QueryParam("rows") Integer rows);

	@GET
	@Path("/{table_name}/column/{column_name}/term/{term}")
	@Produces(ServiceInterface.APPLICATION_JSON_UTF8)
	List<String> getColumnTermKeys(@PathParam("table_name") String table_name,
			@PathParam("column_name") String column_name, @PathParam("term") String term,
			@QueryParam("start") Integer start, @QueryParam("rows") Integer rows);

	@POST
	@Path("/{table_name}/column/{column_name}")
	@Consumes(ServiceInterface.APPLICATION_JSON_UTF8)
	@Produces(ServiceInterface.APPLICATION_JSON_UTF8)
	ColumnDefinition setColumn(@PathParam("table_name") String table_name, @PathParam("column_name") String column_name,
			ColumnDefinition columnDefinition);

	@DELETE
	@Path("/{table_name}/column/{column_name}")
	@Produces(ServiceInterface.APPLICATION_JSON_UTF8)
	Boolean removeColumn(@PathParam("table_name") String table_name, @PathParam("column_name") String column_name);

	@GET
	@Path("/{table_name}/row")
	@Consumes(ServiceInterface.APPLICATION_JSON_UTF8)
	@Produces(ServiceInterface.APPLICATION_JSON_UTF8)
	List<String> getRows(@PathParam("table_name") String table_name, @QueryParam("start") Integer start,
			@QueryParam("rows") Integer rows);

	@POST
	@Path("/{table_name}/row")
	@Consumes(ServiceInterface.APPLICATION_JSON_UTF8)
	@Produces(ServiceInterface.APPLICATION_JSON_UTF8)
	Long upsertRows(@PathParam("table_name") String table_name, List<Map<String, Object>> rows);

	@POST
	@Path("/{table_name}/row")
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(ServiceInterface.APPLICATION_JSON_UTF8)
	Long upsertRows(@PathParam("table_name") String table_name, @QueryParam("buffer") Integer buffer,
			InputStream inpustStream);

	@PUT
	@Path("/{table_name}/row/{row_id}")
	@Consumes(ServiceInterface.APPLICATION_JSON_UTF8)
	@Produces(ServiceInterface.APPLICATION_JSON_UTF8)
	Map<String, Object> upsertRow(@PathParam("table_name") String table_name, @PathParam("row_id") String row_id,
			Map<String, Object> node);

	@GET
	@Path("/{table_name}/row/{row_id}")
	@Produces(ServiceInterface.APPLICATION_JSON_UTF8)
	Map<String, Object> getRow(@PathParam("table_name") String table_name, @PathParam("row_id") String row_id,
			@QueryParam("column") Set<String> columns);

	@POST
	@Path("/{table_name}/rows")
	@Produces(ServiceInterface.APPLICATION_JSON_UTF8)
	List<Map<String, Object>> getRows(@PathParam("table_name") String table_name,
			@QueryParam("column") Set<String> columns, final Set<String> row_ids);

	@DELETE
	@Path("/{table_name}/row/{row_id}")
	@Produces(ServiceInterface.APPLICATION_JSON_UTF8)
	Boolean deleteRow(@PathParam("table_name") String table_name, @PathParam("row_id") String row_id);

	@POST
	@Path("/{table_name}/query")
	@Consumes(ServiceInterface.APPLICATION_JSON_UTF8)
	@Produces(ServiceInterface.APPLICATION_JSON_UTF8)
	TableRequestResult queryRows(@PathParam("table_name") String table_name, TableRequest tableRequest);

	GenericType<SortedSet<String>> sortedSetStringType = new GenericType<SortedSet<String>>() {
	};

	GenericType<List<Object>> listObjectType = new GenericType<List<Object>>() {
	};

	GenericType<List<String>> listStringType = new GenericType<List<String>>() {
	};

	GenericType<Map<String, Object>> mapStringObjectType = new GenericType<Map<String, Object>>() {
	};

	GenericType<List<Map<String, Object>>> listMapStringObjectType = new GenericType<List<Map<String, Object>>>() {
	};

}
