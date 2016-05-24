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
package com.qwazr.database.test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.Files;
import com.qwazr.database.TableBuilder;
import com.qwazr.database.TableServer;
import com.qwazr.database.TableServiceInterface;
import com.qwazr.database.TableSingleClient;
import com.qwazr.database.model.ColumnDefinition;
import com.qwazr.database.model.TableDefinition;
import com.qwazr.database.model.TableRequest;
import com.qwazr.database.model.TableRequestResult;
import com.qwazr.database.store.Table;
import com.qwazr.utils.CharsetUtils;
import com.qwazr.utils.IOUtils;
import com.qwazr.utils.json.JsonMapper;
import com.qwazr.utils.server.RemoteService;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import javax.ws.rs.WebApplicationException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class FullTest {

	public static final String BASE_URL = "http://localhost:9091";
	public static final ColumnDefinition COLUMN_DEF_PASSWORD = getColumnDefinition("column_def_password.json");
	public static final ColumnDefinition COLUMN_DEF_ROLES = getColumnDefinition("column_def_roles.json");
	public static final ColumnDefinition COLUMN_DEF_DPT_ID = new ColumnDefinition(ColumnDefinition.Type.INTEGER,
			ColumnDefinition.Mode.INDEXED);

	public static final Map<String, Object> UPSERT_ROW1 =
			getTypeDef("upsert_row1.json", TableSingleClient.MapStringObjectTypeRef);
	public static final Map<String, Object> UPSERT_ROW2 =
			getTypeDef("upsert_row2.json", TableSingleClient.MapStringObjectTypeRef);
	public static final List<Map<String, Object>> UPSERT_ROWS =
			getTypeDef("upsert_rows.json", TableSingleClient.ListMapStringObjectTypeRef);
	public static final String TABLE_NAME = "test_table";
	public static final String COLUMN_NAME_PASSWORD = "password";
	public static final String COLUMN_NAME_ROLES = "roles";
	public static final String COLUMN_NAME_ROLES2 = "roles2";
	public static final String COLUMN_NAME_DPT_ID = "dptId";
	public static final String ID1 = "one";
	public static final String ID2 = "two";
	public static final String ID3 = "three";
	public static final String ID4 = "four";
	public static final String PASS1 = "password1";
	public static final String PASS2 = "password2";
	public static final String PASS3 = "password3";
	public static final String PASS4 = "password4";
	public static final Set<String> COLUMNS;
	public static final Set<String> COLUMNS_WITHID;

	static {
		COLUMNS = new HashSet<>();
		COLUMNS.add("roles");
		COLUMNS.add("password");
		COLUMNS_WITHID = new HashSet<>(COLUMNS);
		COLUMNS_WITHID.add(Table.ID_COLUMN_NAME);
	}

	@BeforeClass
	public static void startDatabaseServer() throws Exception {
		final File dataDir = Files.createTempDir();
		System.setProperty("QWAZR_DATA", dataDir.getAbsolutePath());
		System.setProperty("LISTEN_ADDR", "localhost");
		System.setProperty("PUBLIC_ADDR", "localhost");
		TableServer.main(new String[]{});
	}

	@Test
	public void test000CreateTable() throws URISyntaxException {
		TableServiceInterface client = getClient();
		TableDefinition tableDefinition = client.createTable(TABLE_NAME);
		Assert.assertNotNull(tableDefinition);
	}

	private void checkColumnDefinitions(ColumnDefinition left, ColumnDefinition right) {
		Assert.assertNotNull(left);
		Assert.assertNotNull(right);
		Assert.assertEquals(left.mode, right.mode);
		Assert.assertEquals(left.type, right.type);
	}

	@Test
	public void test100SetColumns() throws URISyntaxException {
		TableServiceInterface client = getClient();
		ColumnDefinition columnDefinition = client.addColumn(TABLE_NAME, COLUMN_NAME_PASSWORD, COLUMN_DEF_PASSWORD);
		Assert.assertNotNull(columnDefinition);
		checkColumnDefinitions(columnDefinition, COLUMN_DEF_PASSWORD);
		columnDefinition = client.addColumn(TABLE_NAME, COLUMN_NAME_ROLES, COLUMN_DEF_ROLES);
		Assert.assertNotNull(columnDefinition);
		checkColumnDefinitions(columnDefinition, COLUMN_DEF_ROLES);
		columnDefinition = client.addColumn(TABLE_NAME, COLUMN_NAME_ROLES2, COLUMN_DEF_ROLES);
		Assert.assertNotNull(columnDefinition);
		checkColumnDefinitions(columnDefinition, COLUMN_DEF_ROLES);
		columnDefinition = client.addColumn(TABLE_NAME, COLUMN_NAME_DPT_ID, COLUMN_DEF_DPT_ID);
		Assert.assertNotNull(columnDefinition);
		checkColumnDefinitions(columnDefinition, COLUMN_DEF_DPT_ID);
	}

	private void checkColumn(TableServiceInterface client, String columnName, ColumnDefinition columnDefinition) {
		Assert.assertNotNull(columnDefinition);
		ColumnDefinition cd = client.getColumn(TABLE_NAME, columnName);
		Assert.assertNotNull(cd);
		checkColumnDefinitions(cd, columnDefinition);
	}

	@Test
	public void test110getColumn() throws URISyntaxException {
		TableServiceInterface client = getClient();
		checkColumn(client, COLUMN_NAME_PASSWORD, COLUMN_DEF_PASSWORD);
		checkColumn(client, COLUMN_NAME_ROLES, COLUMN_DEF_ROLES);
		checkColumn(client, COLUMN_NAME_ROLES2, COLUMN_DEF_ROLES);
		checkColumn(client, COLUMN_NAME_DPT_ID, COLUMN_DEF_DPT_ID);
	}

	// TODO Not Yet implemented
	public void test130removeColumn() throws URISyntaxException {
		TableServiceInterface client = getClient();
		client.removeColumn(TABLE_NAME, COLUMN_NAME_ROLES2);
		Map<String, ColumnDefinition> columns = client.getColumns(TABLE_NAME);
		Assert.assertNull(columns.get(COLUMN_NAME_ROLES2));
	}

	@Test
	public void test120getColumns() throws URISyntaxException {
		TableServiceInterface client = getClient();
		Map<String, ColumnDefinition> columns = client.getColumns(TABLE_NAME);
		Assert.assertNotNull(columns);
		checkColumnDefinitions(columns.get(COLUMN_NAME_PASSWORD), COLUMN_DEF_PASSWORD);
		checkColumnDefinitions(columns.get(COLUMN_NAME_ROLES), COLUMN_DEF_ROLES);
	}

	@Test
	public void test150MatchAllQueryEmpty() throws URISyntaxException {
		TableServiceInterface client = getClient();
		TableRequest request = new TableRequest(0, 1000, COLUMNS_WITHID, null, null);
		TableRequestResult result = client.queryRows(TABLE_NAME, request);
		Assert.assertNotNull(result);
		Assert.assertEquals(new Long(0), result.count);
	}

	@Test
	public void test300upsertRow() throws URISyntaxException {
		final TableServiceInterface client = getClient();
		Assert.assertNotNull(client.upsertRow(TABLE_NAME, ID1, UPSERT_ROW1));
		Assert.assertNotNull(client.upsertRow(TABLE_NAME, ID2, UPSERT_ROW2));
		checkGetRow("password", PASS1, client.getRow(TABLE_NAME, ID1, COLUMNS));
		checkGetRow("password", PASS2, client.getRow(TABLE_NAME, ID2, COLUMNS));
	}

	@Test
	public void test350upsertRows() throws URISyntaxException {
		final TableServiceInterface client = getClient();
		Long result = client.upsertRows(TABLE_NAME, UPSERT_ROWS);
		Assert.assertNotNull(result);
		Assert.assertEquals((long) result, UPSERT_ROWS.size());
	}

	@Test
	public void test355MatchAllQuery() throws URISyntaxException {
		final TableServiceInterface client = getClient();
		TableRequest request = new TableRequest(0, 1000, COLUMNS_WITHID, null, null);
		TableRequestResult result = client.queryRows(TABLE_NAME, request);
		Assert.assertNotNull(result);
		Assert.assertEquals(new Long(4), result.count);
		Assert.assertNotNull(result.rows);
		Assert.assertEquals(4, result.rows.size());
	}

	@Test
	public void test400FilterQuery() throws URISyntaxException {
		final TableServiceInterface client = getClient();
		final ObjectNode query = JsonMapper.MAPPER.createObjectNode();
		final ArrayNode andGroup = JsonMapper.MAPPER.createArrayNode();
		andGroup.addObject().put(COLUMN_NAME_DPT_ID, 1);
		query.set(TableRequest.AND, andGroup);
		TableRequest request = new TableRequest(0, 1000, COLUMNS_WITHID, null, query);
		TableRequestResult result = client.queryRows(TABLE_NAME, request);
		Assert.assertNotNull(result);
		Assert.assertNotNull(result.count);
		Assert.assertEquals(new Long(3), result.count);
	}

	private void deleteAndCheck(String id) throws URISyntaxException {
		final TableServiceInterface client = getClient();
		Assert.assertTrue(client.deleteRow(TABLE_NAME, id));
		try {
			client.getRow(TABLE_NAME, id, COLUMNS);
			Assert.assertTrue("The 404 exception has not been thrown", false);
		} catch (WebApplicationException e) {
			Assert.assertEquals(404, e.getResponse().getStatus());
		}
	}

	@Test
	public void test360DeleteAndUpsertRow() throws URISyntaxException {
		final TableServiceInterface client = getClient();
		deleteAndCheck(ID1);
		deleteAndCheck(ID2);
		Assert.assertNotNull(client.upsertRow(TABLE_NAME, ID2, UPSERT_ROW2));
		Assert.assertNotNull(client.upsertRow(TABLE_NAME, ID1, UPSERT_ROW1));
		deleteAndCheck(ID3);
		deleteAndCheck(ID4);
		Assert.assertEquals((long) UPSERT_ROWS.size(), (long) client.upsertRows(TABLE_NAME, UPSERT_ROWS));
	}

	private Map<String, Object> checkGetRow(String column, String value, Map<String, Object> row) {
		Assert.assertNotNull(row);
		List<String> values = (List<String>) row.get(column);
		Assert.assertNotNull(values);
		Assert.assertFalse(values.isEmpty());
		Assert.assertEquals(values.get(0), value);
		return row;
	}

	@Test
	public void test400getRow() throws URISyntaxException {
		TableServiceInterface client = getClient();
		checkGetRow("password", PASS3, client.getRow(TABLE_NAME, ID3, COLUMNS));
		checkGetRow("password", PASS4, client.getRow(TABLE_NAME, ID4, COLUMNS));
		Map<String, Object> row = checkGetRow("password", PASS1, client.getRow(TABLE_NAME, ID1, COLUMNS));
		List<String> roles = (List<String>) row.get("roles");
		Assert.assertNotNull(roles);
		Assert.assertEquals(2, roles.size());
	}

	private static final String TB_NAME = "tb_test";
	private static final String[] TB_COLS = {"col1", "col2", "col3", "col4"};

	private TableBuilder getTableBuilder() {
		final TableBuilder builder = new TableBuilder(TB_NAME);
		builder.addColumn(TB_COLS[0], ColumnDefinition.Type.STRING, ColumnDefinition.Mode.INDEXED);
		builder.addColumn(TB_COLS[1], ColumnDefinition.Type.INTEGER, ColumnDefinition.Mode.INDEXED);
		builder.addColumn(TB_COLS[2], ColumnDefinition.Type.DOUBLE, ColumnDefinition.Mode.STORED);
		builder.addColumn(TB_COLS[3], ColumnDefinition.Type.LONG, ColumnDefinition.Mode.STORED);
		return builder;
	}

	private void checkColumns(Map<String, ColumnDefinition> columns, String... cols) {
		Assert.assertNotNull(columns);
		for (String col : cols)
			Assert.assertTrue(columns.containsKey(col));
	}

	@Test
	public void test900tableBuilder() throws URISyntaxException {
		final TableServiceInterface client = getClient();
		getTableBuilder().build(client);
		final Map<String, ColumnDefinition> columns = client.getColumns(TB_NAME);
		checkColumns(columns, TB_COLS);
		Assert.assertEquals(TB_COLS.length, columns.size());
	}

	@Test
	public void test901tableBuilderAddColumn() throws URISyntaxException {
		final TableServiceInterface client = getClient();
		getTableBuilder().addColumn("col5", ColumnDefinition.Type.STRING, ColumnDefinition.Mode.STORED).build(client);
		final Map<String, ColumnDefinition> columns = client.getColumns(TB_NAME);
		checkColumns(columns, TB_COLS);
		checkColumns(columns, "col5");
		Assert.assertEquals(TB_COLS.length + 1, columns.size());
	}

	@Test
	public void test950deleteTable() throws URISyntaxException {
		final TableServiceInterface client = getClient();
		client.deleteTable(TB_NAME);
		try {
			client.getTable(TB_NAME);
			Assert.fail("Table not deleted");
		} catch (WebApplicationException e) {
			Assert.assertEquals(404, e.getResponse().getStatus());
		}
	}

	private static TableServiceInterface CLIENT = null;

	private synchronized TableServiceInterface getClient() throws URISyntaxException {
		if (CLIENT != null)
			return CLIENT;
		CLIENT = new TableSingleClient(new RemoteService(BASE_URL));
		return CLIENT;
	}

	private static ColumnDefinition getColumnDefinition(String res) {
		InputStream is = FullTest.class.getResourceAsStream(res);
		try {
			return ColumnDefinition.newColumnDefinition(IOUtils.toString(is, CharsetUtils.CharsetUTF8));
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			IOUtils.close(is);
		}
	}

	private static <T> T getTypeDef(String res, TypeReference valueTypeRef) {
		InputStream is = FullTest.class.getResourceAsStream(res);
		try {
			return JsonMapper.MAPPER.readValue(is, valueTypeRef);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			IOUtils.close(is);
		}
	}
}
