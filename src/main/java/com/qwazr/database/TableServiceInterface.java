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
import com.qwazr.utils.server.RestApplication;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

@Path("/table")
public interface TableServiceInterface {

	@GET
	@Path("/")
	@Produces(RestApplication.APPLICATION_JSON_UTF8)
	public Set<String> list(@QueryParam("timeout") Integer msTimeOut,
							@QueryParam("local") Boolean local);

	@POST
	@Path("/{table_name}")
	@Consumes(RestApplication.APPLICATION_JSON_UTF8)
	@Produces(RestApplication.APPLICATION_JSON_UTF8)
	public TableDefinition createUpdateTable(
			@PathParam("table_name") String table_name,
			TableDefinition table_def,
			@QueryParam("timeout") Integer msTimeOut,
			@QueryParam("local") Boolean local);

	@GET
	@Path("/{table_name}")
	@Produces(RestApplication.APPLICATION_JSON_UTF8)
	public TableDefinition getTable(@PathParam("table_name") String table_name,
									@QueryParam("timeout") Integer msTimeOut,
									@QueryParam("local") Boolean local);

	@DELETE
	@Path("/{table_name}")
	@Produces(RestApplication.APPLICATION_JSON_UTF8)
	public TableDefinition deleteTable(
			@PathParam("table_name") String table_name,
			@QueryParam("timeout") Integer msTimeOut,
			@QueryParam("local") Boolean local);

	@PUT
	@POST
	@Path("/{table_name}/row")
	@Consumes(RestApplication.APPLICATION_JSON_UTF8)
	@Produces(RestApplication.APPLICATION_JSON_UTF8)
	public Long upsertRows(
			@PathParam("table_name") String table_name,
			LinkedHashMap<String, Object> rows);

	@PUT
	@POST
	@Path("/{table_name}/row")
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(RestApplication.APPLICATION_JSON_UTF8)
	public Long upsertRows(@PathParam("table_name") String table_name,
						   InputStream inpustStream);

	@PUT
	@Path("/{table_name}/row/{row_id}")
	@Consumes(RestApplication.APPLICATION_JSON_UTF8)
	@Produces(RestApplication.APPLICATION_JSON_UTF8)
	public LinkedHashMap<String, Object> upsertRow(
			@PathParam("table_name") String table_name,
			@PathParam("row_id") String row_id, LinkedHashMap<String, Object> node);

	@GET
	@Path("/{table_name}/row/{row_id}")
	@Produces(RestApplication.APPLICATION_JSON_UTF8)
	public LinkedHashMap<String, Object> getRow(@PathParam("table_name") String table_name,
												@PathParam("node_id") String row_id);

	@DELETE
	@Path("/{table_name}/row/{row_id}")
	@Produces(RestApplication.APPLICATION_JSON_UTF8)
	public LinkedHashMap<String, Object> deleteRow(@PathParam("table_name") String table_name,
												   @PathParam("row_id") String row_id);

	@POST
	@Path("/{table_name}/request")
	@Consumes(RestApplication.APPLICATION_JSON_UTF8)
	@Produces(RestApplication.APPLICATION_JSON_UTF8)
	public List<LinkedHashMap<String, Object>> requestNodes(
			@PathParam("table_name") String graph_name, TableRequest request);

}
