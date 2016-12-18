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

import com.qwazr.database.TableServer;
import com.qwazr.database.annotations.AnnotatedTableService;
import com.qwazr.database.model.TableDefinition;
import com.qwazr.database.model.TableQuery;
import com.qwazr.database.model.TableRequest;
import com.qwazr.database.model.TableRequestResult;
import com.qwazr.utils.http.HttpClients;
import org.apache.http.pool.PoolStats;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import javax.ws.rs.WebApplicationException;
import java.net.URISyntaxException;
import java.util.*;

import static com.qwazr.database.test.JsonTest.checkErrorStatusCode;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class JavaTest {

	private static Set<String> COLUMNS;
	private static Set<String> COLUMNS_WITHID;

	static {
		COLUMNS = new HashSet<>();
		COLUMNS.add(JavaRecord.COL_LABEL);
		COLUMNS.add(JavaRecord.COL_DPT);
		COLUMNS.add(JavaRecord.COL_LAST_UPDATE_DATE);
		COLUMNS.add(JavaRecord.COL_CREATION_DATE);
		COLUMNS_WITHID = new HashSet<>(COLUMNS);
		COLUMNS_WITHID.add(TableDefinition.ID_COLUMN_NAME);
	}

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

	private AnnotatedTableService<JavaRecord> getService() throws URISyntaxException {
		return new AnnotatedTableService<>(TestServer.getRemoteClient(), JavaRecord.class);
	}

	@Test
	public void test050CreateTable() throws URISyntaxException {
		final AnnotatedTableService<JavaRecord> service = getService();
		// First call create the table
		service.createUpdateTable();
		Assert.assertNotNull(service.getTable());
		// Second call should do nothing
		service.createUpdateTable();
		Assert.assertNotNull(service.getTable());
	}

	@Test
	public void test100SetColumns() throws URISyntaxException {
		final AnnotatedTableService<JavaRecord> service = getService();
		// First time
		service.createUpdateFields();
		Assert.assertNotNull(service.getColumns());
		Assert.assertNotNull(service.getColumn(JavaRecord.COL_LABEL));
		// Second time
		service.createUpdateFields();
		Assert.assertNotNull(service.getColumns());
		Assert.assertNotNull(service.getColumn(JavaRecord.COL_LABEL));
	}

	@Test
	public void test150MatchAllQueryEmpty() throws URISyntaxException {
		final AnnotatedTableService<JavaRecord> service = getService();
		final TableRequest request = new TableRequest(0, 1000, COLUMNS, null, null);
		final TableRequestResult result = service.queryRows(request);
		Assert.assertNotNull(result);
		Assert.assertEquals(new Long(0), result.count);
	}

	@Test
	public void test300upsertRow() throws ReflectiveOperationException, URISyntaxException {
		final AnnotatedTableService<JavaRecord> service = getService();
		service.upsertRow(ID1, ROW1);
		Assert.assertEquals(ROW1, service.getRow(ID1, COLUMNS));
	}

	@Test
	public void test350upsertRows() throws URISyntaxException {
		final AnnotatedTableService<JavaRecord> service = getService();
		final List<JavaRecord> rows = Arrays.asList(ROW2, ROW3);
		final Long result = service.upsertRows(rows);
		Assert.assertNotNull(result);
		Assert.assertEquals((long) result, rows.size());
	}

	@Test
	public void test355MatchAllQuery() throws URISyntaxException {
		final AnnotatedTableService<JavaRecord> service = getService();
		final TableRequest request = new TableRequest(0, 1000, COLUMNS_WITHID, null, null);
		final TableRequestResult result = service.queryRows(request);
		Assert.assertNotNull(result);
		Assert.assertEquals(new Long(3), result.count);
		Assert.assertNotNull(result.rows);
		Assert.assertEquals(3, result.rows.size());
	}

	@Test
	public void test360DeleteRow() throws ReflectiveOperationException, URISyntaxException {
		final AnnotatedTableService<JavaRecord> service = getService();
		Assert.assertTrue(service.deleteRow(ID1));
		try {
			service.getRow(ID1, COLUMNS);
			Assert.assertTrue("The 404 exception has not been thrown", false);
		} catch (WebApplicationException e) {
			Assert.assertEquals(404, e.getResponse().getStatus());
		}
	}

	private TableRequestResult checkResult(final AnnotatedTableService<JavaRecord> service,
			final TableQuery.Group query, final Long expectedCount, final JavaRecord... rows) {
		final TableRequest request = new TableRequest(0, 100, COLUMNS_WITHID, null, query.build());
		final AnnotatedTableService.TableRequestResultRecords<JavaRecord> result = service.queryRows(request);
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
	public void test400FilterQuery() throws URISyntaxException {
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
	public void test700getRows() throws URISyntaxException {
		final AnnotatedTableService<JavaRecord> service = getService();
		checkRows(service.getRows((Integer) null, null), ID2, ID3);
	}

	@Test
	public void test705getRows() throws URISyntaxException {
		final AnnotatedTableService<JavaRecord> service = getService();
		final Set<String> keys = new LinkedHashSet<>(Arrays.asList(ID3, ID2));
		List<JavaRecord> results = service.getRows(COLUMNS_WITHID, keys);
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
	public void test800getColumnsTerms() throws URISyntaxException {
		final AnnotatedTableService<JavaRecord> service = getService();
		checkColumnsTerms(service.getColumnTerms(JavaRecord.COL_DPT, 0, 100), ROW2.dpt.get(0), ROW2.dpt.get(1),
				ROW2.dpt.get(2), ROW3.dpt.get(0), ROW3.dpt.get(1), ROW3.dpt.get(2));
	}

	@Test
	public void test81getColumnsTermKeys() throws URISyntaxException {
		final AnnotatedTableService<JavaRecord> service = getService();
		checkColumnsTerms(service.getColumnTermKeys(JavaRecord.COL_DPT, ROW2.dpt.get(0).toString(), 0, 100), ID2);
	}

	@Test
	public void test950deleteTable() throws URISyntaxException {
		final AnnotatedTableService<JavaRecord> service = getService();
		service.deleteTable();
		checkErrorStatusCode(() -> service.deleteTable(), 404);
	}

	@Test
	public void testZZZhttpClient() {
		final PoolStats stats = HttpClients.CNX_MANAGER.getTotalStats();
		Assert.assertEquals(0, HttpClients.CNX_MANAGER.getTotalStats().getLeased());
		Assert.assertEquals(0, stats.getPending());
		Assert.assertTrue(stats.getAvailable() > 0);
		TableServer.shutdown();
	}

	@AfterClass
	public static void stopServer() {
		TestServer.shutdown();
	}

}
