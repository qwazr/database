/**
 * Copyright 2015 OpenSearchServer Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qwarz.database;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.qwazr.utils.json.JsonMapper;

public abstract class Query {

	private static final Logger logger = LoggerFactory.getLogger(Query.class);

	static Query prepare(Database database, JsonNode node)
			throws QueryException {
		if (!node.isObject())
			throw new QueryException(
					"Query error: An object was expected. But got "
							+ node.getNodeType() + " Near: " + node.asText());
		if (node.size() == 0)
			throw new QueryException("The query is empty: Near: "
					+ node.asText());
		if (node.size() > 1)
			throw new QueryException(
					"Query error: More than one object has been found. Near: "
							+ node.asText());
		if (node.has("$OR"))
			return new OrGroup(database, node.get("$OR"));
		if (node.has("$AND"))
			return new AndGroup(database, node.get("$AND"));
		return new TermQuery(database, node);
	}

	private final RoaringBitmap bitmap;

	protected Query(Database database) {
		bitmap = new RoaringBitmap();
		System.out.println("New QueryClause " + this);
	}

	private static class TermQuery extends Query {

		private final FieldInterface field;
		private final String value;

		TermQuery(Database database, JsonNode node) {
			super(database);
			Map.Entry<String, JsonNode> entry = node.fields().next();
			if (database != null)
				field = database.getIndexedField(entry.getKey());
			else
				field = null;
			value = entry.getValue().textValue();
		}
	}

	private static abstract class GroupQuery extends Query {

		private final List<Query> queries;

		protected GroupQuery(Database database, JsonNode node)
				throws QueryException {
			super(database);
			if (!node.isArray())
				throw new QueryException("Array expected, but got "
						+ node.getNodeType());

			queries = new ArrayList<Query>(node.size());

			node.forEach(new Consumer<JsonNode>() {

				@Override
				public void accept(JsonNode node) {
					queries.add(Query.prepare(database, node));
				}
			});
		}
	}

	private static class OrGroup extends GroupQuery {

		protected OrGroup(Database database, JsonNode node) {
			super(database, node);
		}

	}

	private static class AndGroup extends GroupQuery {

		protected AndGroup(Database database, JsonNode node) {
			super(database, node);
		}

	}

	public static class QueryException extends RuntimeException {

		/**
		 * 
		 */
		private static final long serialVersionUID = -5566235355622756480L;

		private QueryException(String reason) {
			super(reason);
		}
	}

	public static void main(String[] argv) throws QueryException,
			JsonProcessingException, IOException {
		final String json = "{ \"$OR\": [ { \"$AND\": [ {\"cat\": \"c1\"}, {\"cat\": \"c2\"} ] }, { \"$AND\": [ {\"cat\": \"c3\"}, {\"cat\": \"c4\"} ] }    ] }";
		Query.prepare(null, JsonMapper.MAPPER.readTree(json));
	}
}
