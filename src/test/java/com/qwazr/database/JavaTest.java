/*
 * Copyright 2015-2017 Emmanuel Keller / QWAZR
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

import com.qwazr.database.annotations.AnnotatedTableService;
import com.qwazr.database.annotations.TableRequestResultRecords;
import com.qwazr.database.model.ColumnDefinition;
import com.qwazr.database.model.TableDefinition;
import com.qwazr.database.model.TableQuery;
import com.qwazr.database.model.TableRequest;
import com.qwazr.database.model.TableRequestResult;
import com.qwazr.database.store.KeyStore;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import javax.ws.rs.WebApplicationException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class JavaTest implements TableTestHelper {

	private static String[] COLUMNS =
			{ JavaRecord.COL_LABEL, JavaRecord.COL_DPT, JavaRecord.COL_LAST_UPDATE_DATE, JavaRecord.COL_CREATION_DATE };

	private static Set<String> COLUMNS_SET = new LinkedHashSet<>(Arrays.asList(COLUMNS));

	private static String[] COLUMNS_WITHID = { JavaRecord.COL_LABEL,
			JavaRecord.COL_DPT,
			JavaRecord.COL_LAST_UPDATE_DATE,
			JavaRecord.COL_CREATION_DATE,
			TableDefinition.ID_COLUMN_NAME };

	private static Set<String> COLUMNS_WITHID_SET = new LinkedHashSet<>(Arrays.asList(COLUMNS_WITHID));

	public static final String ID1 = "one";
	public static final String ID2 = "two";
	public static final String ID3 = "three";

	private static JavaRecord ROW1 = new JavaRecord(ID1, 100);
	private static JavaRecord ROW2 = new JavaRecord(ID2, 200);
	private static JavaRecord ROW3 = new JavaRecord(ID3, 300);

	@Test
	public void test000startServer() throws Exception {
		TestServer.start();
	}

	private AnnotatedTableService<JavaRecord> getService() throws URISyntaxException, NoSuchMethodException {
		return new AnnotatedTableService<>(TestServer.getRemoteClient(), JavaRecord.class);
	}

	@Test
	public void test050CreateTable() throws URISyntaxException, NoSuchMethodException {
		final AnnotatedTableService<JavaRecord> service = getService();

		final TableDefinition tableDefinition = new TableDefinition(KeyStore.Impl.leveldb, null);
		// First call create the table
		service.createUpdateTable();
		checkStatus(service.getTableStatus(), 0, tableDefinition);
		// Second call should do nothing
		service.createUpdateTable();
		checkStatus(service.getTableStatus(), 0, tableDefinition);
	}

	@Test
	public void test100SetColumns() throws URISyntaxException, NoSuchMethodException {
		final AnnotatedTableService<JavaRecord> service = getService();
		// First time
		service.createUpdateFields();
		Assert.assertNotNull(service.getColumns());
		checkColumn(service.getColumn(JavaRecord.COL_LABEL), ColumnDefinition.Type.STRING,
				ColumnDefinition.Mode.STORED);
		checkColumn(service.getColumn(JavaRecord.COL_DPT), ColumnDefinition.Type.INTEGER,
				ColumnDefinition.Mode.INDEXED);
		// Second time
		service.createUpdateFields();
		Assert.assertNotNull(service.getColumns());
		checkColumn(service.getColumn(JavaRecord.COL_LABEL), ColumnDefinition.Type.STRING,
				ColumnDefinition.Mode.STORED);
		checkColumn(service.getColumn(JavaRecord.COL_DPT), ColumnDefinition.Type.INTEGER,
				ColumnDefinition.Mode.INDEXED);
	}

	@Test
	public void test150MatchAllQueryEmpty() throws URISyntaxException, ReflectiveOperationException, IOException {
		final AnnotatedTableService<JavaRecord> service = getService();
		final TableRequest request = TableRequest.from(0, 1000).column(COLUMNS).build();
		final TableRequestResult result = service.queryRows(request);
		Assert.assertNotNull(result);
		Assert.assertEquals(new Long(0), result.count);
	}

	@Test
	public void test300upsertRow() throws ReflectiveOperationException, URISyntaxException, IOException {
		final AnnotatedTableService<JavaRecord> service = getService();
		service.upsertRow(ID1, ROW1);
		Assert.assertEquals(ROW1, service.getRow(ID1, COLUMNS_SET));
	}

	@Test
	public void test350upsertRows() throws URISyntaxException, NoSuchMethodException {
		final AnnotatedTableService<JavaRecord> service = getService();
		final List<JavaRecord> rows = Arrays.asList(ROW2, ROW3);
		final Long result = service.upsertRows(rows);
		Assert.assertNotNull(result);
		Assert.assertEquals((long) result, rows.size());
	}

	@Test
	public void test355MatchAllQuery() throws URISyntaxException, ReflectiveOperationException, IOException {
		final AnnotatedTableService<JavaRecord> service = getService();
		final TableRequest request = TableRequest.from(0, 1000).column(COLUMNS_WITHID).build();
		final TableRequestResult result = service.queryRows(request);
		Assert.assertNotNull(result);
		Assert.assertEquals(new Long(3), result.count);
		Assert.assertNotNull(result.rows);
		Assert.assertEquals(3, result.rows.size());
	}

	@Test
	public void test360DeleteRow() throws ReflectiveOperationException, URISyntaxException, IOException {
		final AnnotatedTableService<JavaRecord> service = getService();
		Assert.assertTrue(service.deleteRow(ID1));
		try {
			service.getRow(ID1, COLUMNS_SET);
			Assert.assertTrue("The 404 exception has not been thrown", false);
		} catch (WebApplicationException e) {
			Assert.assertEquals(404, e.getResponse().getStatus());
		}
	}

	private TableRequestResult checkResult(final AnnotatedTableService<JavaRecord> service,
			final TableQuery.Group query, final Long expectedCount, final JavaRecord... rows)
			throws IOException, ReflectiveOperationException {
		final TableRequest request = TableRequest.from(0, 100).column(COLUMNS_WITHID).query(query).build();
		final TableRequestResultRecords<JavaRecord> result = service.queryRows(request);
		Assert.assertNotNull(result);
		Assert.assertNotNull(result.count);
		if (expectedCount != null)
			Assert.assertEquals(expectedCount, result.count);
		if (rows != null && rows.length > 0) {
			Assert.assertEquals(rows.length, result.rows.size());
			int i = 0;
			for (JavaRecord record : result.records)
				Assert.assertEquals(rows[i++], record);
		}
		return result;
	}

	@Test
	public void test400FilterQuery() throws URISyntaxException, ReflectiveOperationException, IOException {
		final AnnotatedTableService<JavaRecord> service = getService();
		checkResult(service, new TableQuery.And().add(JavaRecord.COL_DPT, ROW2.dpt.get(0)), 1L);
		checkResult(service,
				new TableQuery.Or().add(JavaRecord.COL_DPT, ROW3.dpt.get(0)).add(JavaRecord.COL_DPT, ROW2.dpt.get(0)),
				2L, ROW2, ROW3);
	}

	private void checkRows(final List<String> rows, final String... keys) {
		Assert.assertNotNull(rows);
		Assert.assertEquals(keys.length, rows.size());
		int i = 0;
		for (String key : keys)
			Assert.assertEquals(key, rows.get(i++));
	}

	@Test
	public void test700getRows() throws URISyntaxException, NoSuchMethodException {
		final AnnotatedTableService<JavaRecord> service = getService();
		checkRows(service.getRows((Integer) null, null), ID2, ID3);
	}

	@Test
	public void test705getRows() throws URISyntaxException, ReflectiveOperationException, IOException {
		final AnnotatedTableService<JavaRecord> service = getService();
		final Set<String> keys = new LinkedHashSet<>(Arrays.asList(ID3, ID2));
		List<JavaRecord> results = service.getRows(COLUMNS_WITHID_SET, keys);
		Assert.assertNotNull(results);
		Assert.assertEquals(keys.size(), results.size());
		int i = 0;
		for (String key : keys)
			Assert.assertEquals(key, results.get(i++).id);
	}

	private void checkColumnsTerms(List<?> terms, Object... expectedTerms) {
		Assert.assertNotNull(terms);
		Assert.assertEquals(expectedTerms.length, terms.size());
		for (Object expectedTerm : expectedTerms)
			Assert.assertTrue(terms.contains(expectedTerm));
	}

	@Test
	public void test800getColumnsTerms() throws URISyntaxException, NoSuchMethodException {
		final AnnotatedTableService<JavaRecord> service = getService();
		checkColumnsTerms(service.getColumnTerms(JavaRecord.COL_DPT, 0, 100), ROW2.dpt.get(0), ROW2.dpt.get(1),
				ROW2.dpt.get(2), ROW3.dpt.get(0), ROW3.dpt.get(1), ROW3.dpt.get(2));
	}

	@Test
	public void test81getColumnsTermKeys() throws URISyntaxException, NoSuchMethodException {
		final AnnotatedTableService<JavaRecord> service = getService();
		checkColumnsTerms(service.getColumnTermKeys(JavaRecord.COL_DPT, ROW2.dpt.get(0).toString(), 0, 100), ID2);
	}

	@Test
	public void test950deleteTable() throws URISyntaxException, NoSuchMethodException {
		final AnnotatedTableService<JavaRecord> service = getService();
		service.deleteTable();
		JsonTest.checkErrorStatusCode(() -> service.deleteTable(), 404);
	}

	@AfterClass
	public static void stopServer() {
		TableServer.shutdown();
		TestServer.shutdown();
	}

}
