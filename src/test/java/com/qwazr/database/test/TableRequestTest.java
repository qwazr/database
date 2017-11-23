package com.qwazr.database.test;

import com.qwazr.database.model.TableQuery;
import com.qwazr.database.model.TableRequest;
import com.qwazr.utils.ObjectMappers;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashSet;

public class TableRequestTest {

	@Test
	public void test() throws IOException {
		final TableRequest request = new TableRequest(0, 100, new LinkedHashSet<>(Arrays.asList("id")),
				new LinkedHashSet<>(Arrays.asList("tag")), new TableQuery.And().add("id", "2").add("tag", 2));
		final String json = ObjectMappers.JSON.writeValueAsString(request);
		Assert.assertNotNull(json);
		final TableRequest request2 = ObjectMappers.JSON.readValue(json, TableRequest.class);
	}
}
