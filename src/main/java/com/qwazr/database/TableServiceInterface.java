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

import com.qwazr.database.model.ColumnDefinition;
import com.qwazr.database.model.TableDefinition;
import com.qwazr.database.model.TableRequest;
import com.qwazr.database.model.TableRequestResult;
import com.qwazr.utils.server.ServiceInterface;
import com.qwazr.utils.server.ServiceName;

import javax.annotation.security.RolesAllowed;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RolesAllowed(TableManager.SERVICE_NAME_TABLE)
@Path("/table")
@ServiceName(TableManager.SERVICE_NAME_TABLE)
public interface TableServiceInterface extends ServiceInterface {

	@GET
	@Path("/")
	@Produces(ServiceInterface.APPLICATION_JSON_UTF8)
	Set<String> list(@QueryParam("timeout") Integer msTimeOut, @QueryParam("local") Boolean local);

	@POST
	@Path("/{table_name}")
	@Consumes(ServiceInterface.APPLICATION_JSON_UTF8)
	@Produces(ServiceInterface.APPLICATION_JSON_UTF8)
	TableDefinition createTable(@PathParam("table_name") String table_name);

	@GET
	@Path("/{table_name}")
	@Produces(ServiceInterface.APPLICATION_JSON_UTF8)
	TableDefinition getTable(@PathParam("table_name") String table_name);

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

	@POST
	@Path("/{table_name}/column/{column_name}")
	@Consumes(ServiceInterface.APPLICATION_JSON_UTF8)
	@Produces(ServiceInterface.APPLICATION_JSON_UTF8)
	ColumnDefinition addColumn(@PathParam("table_name") String table_name, @PathParam("column_name") String column_name,
					ColumnDefinition columnDefinition);

	@DELETE
	@Path("/{table_name}/column/{column_name}")
	@Produces(ServiceInterface.APPLICATION_JSON_UTF8)
	Boolean removeColumn(@PathParam("table_name") String table_name, @PathParam("column_name") String column_name);

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

	@DELETE
	@Path("/{table_name}/row/{row_id}")
	@Produces(ServiceInterface.APPLICATION_JSON_UTF8)
	Boolean deleteRow(@PathParam("table_name") String table_name, @PathParam("row_id") String row_id);

	@POST
	@Path("/{table_name}/query")
	@Consumes(ServiceInterface.APPLICATION_JSON_UTF8)
	@Produces(ServiceInterface.APPLICATION_JSON_UTF8)
	TableRequestResult queryRows(@PathParam("table_name") String graph_name, TableRequest request);

}
