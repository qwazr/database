/*
 * Copyright 2015-2017 Emmanuel Keller / QWAZR
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

import com.qwazr.database.model.TableQuery;
import com.qwazr.server.ServerException;
import com.qwazr.utils.concurrent.RunnablePool;
import com.qwazr.utils.concurrent.RunnablePoolException;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

public interface Query {

	static Query prepare(final TableQuery query, final QueryHook queryHook) throws QueryException {
		final Query newQuery;
		if (query instanceof TableQuery.Group) {
			if (query instanceof TableQuery.Or)
				newQuery = new OrGroup((TableQuery.Or) query, queryHook);
			else if (query instanceof TableQuery.And)
				newQuery = new AndGroup((TableQuery.And) query, queryHook);
			else
				newQuery = null;
		} else if (query instanceof TableQuery.Term) {
			if (query instanceof TableQuery.StringTerm)
				newQuery = new TermQuery<>((TableQuery.StringTerm) query);
			else if (query instanceof TableQuery.LongTerm)
				newQuery = new TermQuery<>((TableQuery.LongTerm) query);
			else if (query instanceof TableQuery.IntegerTerm)
				newQuery = new TermQuery<>((TableQuery.IntegerTerm) query);
			else if (query instanceof TableQuery.DoubleTerm)
				newQuery = new TermQuery<>((TableQuery.DoubleTerm) query);
			else if (query instanceof TableQuery.FloatTerm)
				newQuery = new TermQuery<>((TableQuery.FloatTerm) query);
			else
				newQuery = null;
		} else
			newQuery = null;
		if (newQuery == null)
			throw new QueryException("Unknown query type: " + query);
		if (queryHook != null)
			queryHook.query(newQuery);
		return newQuery;
	}

	RoaringBitmap execute(final QueryContext context, final ExecutorService executor) throws IOException;

	class TermQuery<T> implements Query {

		private volatile String field;

		private final T value;

		public TermQuery(final TableQuery.Term<T> termQuery) {
			Objects.requireNonNull(termQuery, "The term query is null");
			this.field = termQuery.column;
			this.value = termQuery.value;
		}

		final public String getField() {
			return field;
		}

		final public void setField(String newField) {
			this.field = newField;
		}

		@Override
		final public RoaringBitmap execute(final QueryContext context, final ExecutorService executor)
				throws IOException {
			final RoaringBitmap bitset = context.getIndexedBitset(field, value);
			if (bitset == null)
				return new RoaringBitmap();
			return bitset;
		}

		final public T getValue() {
			return value;
		}
	}

	abstract class GroupQuery implements Query {

		private final Collection<Query> queries;

		protected GroupQuery(TableQuery.Group groupQuery, QueryHook queryHook) throws QueryException {
			queries = new ArrayList<>(groupQuery.queries.size());
			groupQuery.queries.forEach(n -> this.queries.add(Query.prepare(n, queryHook)));
		}

		final public void add(Query query) {
			queries.add(query);
		}

		final RoaringBitmap execute(final QueryContext context, final ExecutorService executor,
				final BiConsumer<RoaringBitmap, RoaringBitmap> reduce) throws IOException {
			try (final RunnablePool<RoaringBitmap> runnablePool = new RunnablePool<>(executor)) {
				final RoaringBitmap finalBitmap = new RoaringBitmap();
				queries.forEach(query -> runnablePool.submit(() -> {
					final RoaringBitmap bitmap = query.execute(context, executor);
					if (bitmap != null) {
						synchronized (finalBitmap) {
							reduce.accept(bitmap, finalBitmap);
						}
					}
					return bitmap;
				}));
				return finalBitmap;
			} catch (RunnablePoolException e) {
				throw ServerException.of(e);
			}
		}
	}

	final class OrGroup extends GroupQuery {

		protected OrGroup(final TableQuery.Or orQuery, QueryHook queryHook) {
			super(orQuery, queryHook);
		}

		@Override
		final public RoaringBitmap execute(final QueryContext context, final ExecutorService executor)
				throws IOException {
			return execute(context, executor, (bitmap, finalBitmap) -> finalBitmap.or(bitmap));
		}
	}

	class AndGroup extends GroupQuery {

		protected AndGroup(final TableQuery.And andQuery, final QueryHook queryHook) {
			super(andQuery, queryHook);
		}

		@Override
		final public RoaringBitmap execute(final QueryContext context, final ExecutorService executor)
				throws IOException {
			final AtomicBoolean first = new AtomicBoolean(true);
			return execute(context, executor, (bitmap, finalBitmap) -> {
				if (first.getAndSet(false))
					finalBitmap.or(bitmap);
				else
					finalBitmap.and(bitmap);
			});
		}
	}

	class QueryException extends RuntimeException {

		/**
		 *
		 */
		private static final long serialVersionUID = -5566235355622756480L;

		private QueryException(final String reason) {
			super(reason);
		}
	}

	interface QueryHook {

		void query(Query query);
	}

}
