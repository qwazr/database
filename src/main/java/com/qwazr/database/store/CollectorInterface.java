/**
 * Copyright 2014-2015 Emmanuel Keller / QWAZR
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
 **/
package com.qwazr.database.store;

import com.qwazr.database.model.ColumnDefinition;
import com.qwazr.database.store.keys.ColumnStoreKey;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public interface CollectorInterface {

    static Collector build() {
	return new Collector(null);
    }

    void collect(RoaringBitmap finalBitmap) throws IOException, DatabaseException;

    void collect(int docId) throws IOException, DatabaseException;

    int getCount();

    DocumentsCollector documents(Collection<Integer> documentIds);

    FacetsCollector facets(QueryContext context, ColumnDefinition.Internal columnDef,
		    Map<Object, LongCounter> termCounter);

    ScoresCollector scores();

    abstract class CollectorAbstract implements CollectorInterface {

	protected final CollectorInterface parent;

	private CollectorAbstract(CollectorInterface parent) {
	    this.parent = parent;
	}

	@Override
	final public DocumentsCollector documents(Collection<Integer> documentIds) {
	    return new DocumentsCollector(this, documentIds);
	}

	@Override
	final public FacetsCollector facets(QueryContext context, ColumnDefinition.Internal columnDef,
			Map<Object, LongCounter> termCounter) {
	    return new FacetsCollector(this, context, columnDef, termCounter);
	}

	@Override
	final public ScoresCollector scores() {
	    return new ScoresCollector(this);
	}

	@Override
	final public void collect(RoaringBitmap bitmap) throws IOException, DatabaseException {
	    for (Integer docId : bitmap)
		collect(docId);
	}

	@Override
	public int getCount() {
	    return parent.getCount();
	}
    }

    class Collector extends CollectorAbstract {

	private int count;

	private Collector(CollectorInterface parent) {
	    super(parent);
	    count = 0;
	}

	@Override
	public void collect(int docId) {
	    count++;
	}

	@Override
	final public int getCount() {
	    return count;
	}

    }

    class DocumentsCollector extends CollectorAbstract {

	private final Collection<Integer> documentIds;

	private DocumentsCollector(CollectorInterface parent, Collection<Integer> documentIds) {
	    super(parent);
	    this.documentIds = documentIds;
	}

	@Override
	final public void collect(int docId) throws IOException, DatabaseException {
	    documentIds.add(docId);
	    parent.collect(docId);
	}
    }

    class LongCounter {
	public long count = 1;
    }

    class FacetsCollector extends CollectorAbstract implements ValueConsumer {

	private final QueryContext context;
	private final ColumnDefinition.Internal columnDef;
	private final Map<Object, LongCounter> termCounter;

	private FacetsCollector(CollectorInterface parent, QueryContext context, ColumnDefinition.Internal columnDef,
			Map<Object, LongCounter> termCounter) {
	    super(parent);
	    this.context = context;
	    this.columnDef = columnDef;
	    this.termCounter = termCounter;
	}

	@Override
	final public void collect(int docId) throws IOException, DatabaseException {
	    parent.collect(docId);
	    ColumnStoreKey.newInstance(columnDef, docId).forEach(context.store, this);
	}

	@Override
	final public void consume(final double value) {
	    LongCounter counter = termCounter.get(value);
	    if (counter == null)
		termCounter.put(value, new LongCounter());
	    else
		counter.count++;
	}

	@Override
	public void consume(long value) {
	    LongCounter counter = termCounter.get(value);
	    if (counter == null)
		termCounter.put(value, new LongCounter());
	    else
		counter.count++;
	}

	@Override
	public void consume(float value) {
	    LongCounter counter = termCounter.get(value);
	    if (counter == null)
		termCounter.put(value, new LongCounter());
	    else
		counter.count++;
	}

	@Override
	public void consume(String value) {
	    LongCounter counter = termCounter.get(value);
	    if (counter == null)
		termCounter.put(value, new LongCounter());
	    else
		counter.count++;
	}
    }

    // TODO Make the implementation
	class ScoresCollector extends CollectorAbstract {

	private ScoresCollector(CollectorInterface parent) {
	    super(parent);
	}

	@Override
	final public void collect(int docId) throws IOException, DatabaseException {
	    parent.collect(docId);
	}
    }

}
