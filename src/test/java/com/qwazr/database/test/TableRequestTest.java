package com.qwazr.database.test;

import com.qwazr.database.model.TableQuery;
import com.qwazr.database.model.TableRequest;
import com.qwazr.utils.ObjectMappers;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TableRequestTest {

	@Test
	public void test() throws IOException {
		final TableRequest request = TableRequest.from(0, 100)
				.column("id")
				.counter("tag")
				.query(new TableQuery.And().add("id", "2").add("tag", 2))
				.build();
		final String json = ObjectMappers.JSON.writeValueAsString(request);
		Assert.assertNotNull(json);
		final TableRequest request2 = ObjectMappers.JSON.readValue(json, TableRequest.class);
		Assert.assertEquals(request, request2);
	}
}
