/**
 * Copyright 2015-2016 Emmanuel Keller / QWAZR
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
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
import com.qwazr.utils.server.RestApplication;

import javax.annotation.security.RolesAllowed;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RolesAllowed(TableServer.SERVICE_NAME_TABLE)
@Path("/table")
public interface TableServiceInterface {

	@GET
	@Path("/")
	@Produces(RestApplication.APPLICATION_JSON_UTF8)
	Set<String> list(@QueryParam("timeout") Integer msTimeOut, @QueryParam("local") Boolean local);

	@POST
	@Path("/{table_name}")
	@Consumes(RestApplication.APPLICATION_JSON_UTF8)
	@Produces(RestApplication.APPLICATION_JSON_UTF8)
	TableDefinition createTable(@PathParam("table_name") String table_name, @QueryParam("timeout") Integer msTimeOut,
					@QueryParam("local") Boolean local);

	@GET
	@Path("/{table_name}")
	@Produces(RestApplication.APPLICATION_JSON_UTF8)
	TableDefinition getTable(@PathParam("table_name") String table_name, @QueryParam("timeout") Integer msTimeOut,
					@QueryParam("local") Boolean local);

	@DELETE
	@Path("/{table_name}")
	@Produces(RestApplication.APPLICATION_JSON_UTF8)
	Boolean deleteTable(@PathParam("table_name") String table_name, @QueryParam("timeout") Integer msTimeOut,
					@QueryParam("local") Boolean local);

	@GET
	@Path("/{table_name}/column")
	@Produces(RestApplication.APPLICATION_JSON_UTF8)
	Map<String, ColumnDefinition> getColumns(@PathParam("table_name") String table_name,
					@QueryParam("timeout") Integer msTimeOut, @QueryParam("local") Boolean local);

	@GET
	@Path("/{table_name}/column/{column_name}")
	@Produces(RestApplication.APPLICATION_JSON_UTF8)
	ColumnDefinition getColumn(@PathParam("table_name") String table_name, @PathParam("column_name") String column_name,
					@QueryParam("timeout") Integer msTimeOut, @QueryParam("local") Boolean local);

	@POST
	@Path("/{table_name}/column/{column_name}")
	@Consumes(RestApplication.APPLICATION_JSON_UTF8)
	@Produces(RestApplication.APPLICATION_JSON_UTF8)
	ColumnDefinition addColumn(@PathParam("table_name") String table_name, @PathParam("column_name") String column_name,
					ColumnDefinition columnDefinition, @QueryParam("timeout") Integer msTimeOut,
					@QueryParam("local") Boolean local);

	@DELETE
	@Path("/{table_name}/column/{column_name}")
	@Produces(RestApplication.APPLICATION_JSON_UTF8)
	Boolean removeColumn(@PathParam("table_name") String table_name, @PathParam("column_name") String column_name,
					@QueryParam("timeout") Integer msTimeOut, @QueryParam("local") Boolean local);

	@PUT
	@POST
	@Path("/{table_name}/row")
	@Consumes(RestApplication.APPLICATION_JSON_UTF8)
	@Produces(RestApplication.APPLICATION_JSON_UTF8)
	Long upsertRows(@PathParam("table_name") String table_name, List<Map<String, Object>> rows);

	@PUT
	@POST
	@Path("/{table_name}/row")
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(RestApplication.APPLICATION_JSON_UTF8)
	Long upsertRows(@PathParam("table_name") String table_name, @QueryParam("buffer") Integer buffer,
					InputStream inpustStream);

	@PUT
	@Path("/{table_name}/row/{row_id}")
	@Consumes(RestApplication.APPLICATION_JSON_UTF8)
	@Produces(RestApplication.APPLICATION_JSON_UTF8)
	Map<String, Object> upsertRow(@PathParam("table_name") String table_name, @PathParam("row_id") String row_id,
					Map<String, Object> node);

	@GET
	@Path("/{table_name}/row/{row_id}")
	@Produces(RestApplication.APPLICATION_JSON_UTF8)
	Map<String, Object> getRow(@PathParam("table_name") String table_name, @PathParam("row_id") String row_id,
					@QueryParam("column") Set<String> columns);

	@DELETE
	@Path("/{table_name}/row/{row_id}")
	@Produces(RestApplication.APPLICATION_JSON_UTF8)
	Boolean deleteRow(@PathParam("table_name") String table_name, @PathParam("row_id") String row_id);

	@POST
	@Path("/{table_name}/query")
	@Consumes(RestApplication.APPLICATION_JSON_UTF8)
	@Produces(RestApplication.APPLICATION_JSON_UTF8)
	TableRequestResult queryRows(@PathParam("table_name") String graph_name, TableRequest request);

}
