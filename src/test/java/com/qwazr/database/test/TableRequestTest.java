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
