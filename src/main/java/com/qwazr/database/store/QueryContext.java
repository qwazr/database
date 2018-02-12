/*
 * Copyright 2015-2018 Emmanuel Keller / QWAZR
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

import com.qwazr.database.model.ColumnDefinition;
import com.qwazr.database.model.InternalColumnDefinition;
import com.qwazr.database.store.keys.ColumnIndexKey;
import com.qwazr.database.store.keys.ColumnStoreKey;
import com.qwazr.database.store.keys.PrimaryIdsKey;
import com.qwazr.server.ServerException;
import org.roaringbitmap.RoaringBitmap;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Map;

public class QueryContext {

	final KeyStore store;
	final Map<String, InternalColumnDefinition> columns;

	QueryContext(final KeyStore store, final Map<String, InternalColumnDefinition> columns) {
		this.store = store;
		this.columns = columns;
	}

	public final RoaringBitmap getIndexedBitset(final String columnName, final Object value) throws IOException {
		final InternalColumnDefinition colDef = columns.get(columnName);
		if (colDef == null)
			throw new ServerException(Response.Status.NOT_ACCEPTABLE, "Unknown column: " + columnName);
		if (colDef.mode != ColumnDefinition.Mode.INDEXED)
			throw new ServerException(Response.Status.NOT_ACCEPTABLE, "The column is not indexed: " + columnName);
		return ColumnIndexKey.newInstance(colDef, value).getValue(store);
	}

	public final CollectorInterface newFacetCollector(final CollectorInterface collector, final String columnName,
			final Map<Object, CollectorInterface.LongCounter> facetMap) {
		return collector.facets(this, columns.get(columnName), facetMap);
	}

	public Integer getExistingDocId(final String key) throws IOException {
		return new PrimaryIdsKey(key).getValue(store);
	}

	public void consumeFirstValue(final String columnName, final int docId, final ValueConsumer consumer)
			throws IOException {
		ColumnStoreKey.newInstance(columns.get(columnName), docId).forFirst(store, consumer);
	}
}
