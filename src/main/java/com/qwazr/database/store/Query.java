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
package com.qwazr.database.store;

import com.fasterxml.jackson.databind.JsonNode;
import com.qwazr.utils.concurrent.ThreadUtils;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class Query {

	final public static Query prepare(JsonNode node, QueryHook queryHook) throws QueryException {
		if (!node.isObject())
			throw new QueryException(
					"Query error: An object was expected. But got " + node.getNodeType() + " Near: " + node.asText());
		if (node.size() == 0)
			throw new QueryException("The query is empty: Near: " + node.asText());
		if (node.size() > 1)
			throw new QueryException("Query error: More than one object has been found. Near: " + node.asText());
		Query newQuery = null;
		if (node.has("$OR"))
			newQuery = new OrGroup(node.get("$OR"), queryHook);
		else if (node.has("$AND"))
			newQuery = new AndGroup(node.get("$AND"), queryHook);
		else {
			Map.Entry<String, JsonNode> entry = node.fields().next();
			String field = entry.getKey();
			JsonNode valueNode = entry.getValue();
			if (valueNode.isTextual())
				newQuery = new TermQuery<String>(field, valueNode.asText());
			else if (node.isFloatingPointNumber())
				newQuery = new TermQuery<Double>(field, valueNode.asDouble());
			else if (node.isIntegralNumber())
				newQuery = new TermQuery<Long>(field, valueNode.asLong());
			else
				throw new QueryException("Unexpected value: " + field + "  Type: " + valueNode.getNodeType());
		}
		if (queryHook != null)
			queryHook.query(newQuery);
		return newQuery;
	}

	abstract RoaringBitmap execute(final QueryContext context, final ExecutorService executor) throws IOException;

	public static class TermQuery<T> extends Query {

		private String field;
		private final T value;

		public TermQuery(final String field, final T value) {
			super();
			this.field = field;
			this.value = value;
		}

		@Override
		final RoaringBitmap execute(final QueryContext context, final ExecutorService executor) throws IOException {
			RoaringBitmap bitset = context.getIndexedBitset(field);
			if (bitset == null)
				bitset = new RoaringBitmap();
			return bitset;
		}

		final public void setField(String field) {
			this.field = field;
		}

		final public String getField() {
			return field;
		}

		final public T getValue() {
			return value;
		}
	}

	static abstract class GroupQuery extends Query {

		protected final List<Query> queries;

		protected GroupQuery(JsonNode node, QueryHook queryHook) throws QueryException {
			if (!node.isArray())
				throw new QueryException("Array expected, but got " + node.getNodeType());
			queries = new ArrayList<>(node.size());
			node.forEach(n -> queries.add(Query.prepare(n, queryHook)));
		}

		protected GroupQuery() {
			queries = new ArrayList<Query>();
		}

		final public void add(Query query) {
			queries.add(query);
		}
	}

	public static class OrGroup extends GroupQuery {

		protected OrGroup(JsonNode node, QueryHook queryHook) {
			super(node, queryHook);
		}

		public OrGroup() {
		}

		@Override
		final RoaringBitmap execute(final QueryContext context, final ExecutorService executor) throws IOException {
			try {
				final RoaringBitmap finalBitmap = new RoaringBitmap();
				ThreadUtils.parallel(queries, query -> {
					final RoaringBitmap bitmap = query.execute(context, executor);
					if (bitmap == null)
						return;
					synchronized (finalBitmap) {
						finalBitmap.or(bitmap);
					}
				});
				return finalBitmap;
			} catch (Exception e) {
				if (e instanceof IOException)
					throw (IOException) e;
				else
					throw new RuntimeException(e);
			}
		}

	}

	public static class AndGroup extends GroupQuery {

		protected AndGroup(JsonNode node, QueryHook queryHook) {
			super(node, queryHook);
		}

		public AndGroup() {
		}

		@Override
		final RoaringBitmap execute(final QueryContext context, final ExecutorService executor) throws IOException {
			try {
				final RoaringBitmap finalBitmap = new RoaringBitmap();
				final AtomicBoolean first = new AtomicBoolean(true);
				ThreadUtils.parallel(queries, query -> {
					final RoaringBitmap bitmap = query.execute(context, executor);
					if (bitmap == null)
						return;
					synchronized (finalBitmap) {
						if (first.getAndSet(false))
							finalBitmap.or(bitmap);
						else
							finalBitmap.and(bitmap);
					}
				});
				return finalBitmap;
			} catch (Exception e) {
				if (e instanceof IOException)
					throw (IOException) e;
				else
					throw new RuntimeException(e);
			}
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

	public interface QueryHook {

		void query(Query query);
	}

}
