/**
 * Copyright 2014-2015 Emmanuel Keller / QWAZR
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.qwazr.database;

import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public interface CollectorInterface {

	static Collector build() {
		return new Collector(null);
	}

	void collect(RoaringBitmap finalBitmap) throws IOException;

	void collect(int docId) throws IOException;

	int getCount();

	DocumentsCollector documents(Collection<Integer> documentIds);

	FacetsCollector facets(Map<Integer, byte[]> termVectorMap,
						   Map<Integer, LongCounter> termCounter);

	ScoresCollector scores();

	static abstract class CollectorAbstract implements CollectorInterface {

		protected final CollectorInterface parent;

		private CollectorAbstract(CollectorInterface parent) {
			this.parent = parent;
		}

		@Override
		final public DocumentsCollector documents(
				Collection<Integer> documentIds) {
			return new DocumentsCollector(this, documentIds);
		}

		@Override
		final public FacetsCollector facets(Map<Integer, byte[]> termVectorMap,
											Map<Integer, LongCounter> termCounter) {
			return new FacetsCollector(this, termVectorMap, termCounter);
		}

		@Override
		final public ScoresCollector scores() {
			return new ScoresCollector(this);
		}

		@Override
		final public void collect(RoaringBitmap bitmap) throws IOException {
			for (Integer docId : bitmap)
				collect(docId);
		}

		@Override
		public int getCount() {
			return parent.getCount();
		}
	}

	static class Collector extends CollectorAbstract {

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

	static class DocumentsCollector extends CollectorAbstract {

		private final Collection<Integer> documentIds;

		private DocumentsCollector(CollectorInterface parent,
								   Collection<Integer> documentIds) {
			super(parent);
			this.documentIds = documentIds;
		}

		@Override
		final public void collect(int docId) throws IOException {
			documentIds.add(docId);
			parent.collect(docId);
		}
	}

	public static class LongCounter {
		public long count = 1;
	}

	static class FacetsCollector extends CollectorAbstract {

		private Map<Integer, byte[]> termVectorMap;
		private final Map<Integer, LongCounter> termCounter;

		private FacetsCollector(CollectorInterface parent,
								Map<Integer, byte[]> termVectorMap,
								Map<Integer, LongCounter> termCounter) {
			super(parent);
			this.termVectorMap = termVectorMap;
			this.termCounter = termCounter;
		}

		@Override
		final public void collect(int docId) throws IOException {
			parent.collect(docId);
			int[] termIdArray = IndexedColumn.getIntArrayOrNull(termVectorMap
					.get(docId));
			if (termIdArray == null)
				return;
			for (int termId : termIdArray) {
				LongCounter counter = termCounter.get(termId);
				if (counter == null)
					termCounter.put(termId, new LongCounter());
				else
					counter.count++;
			}
		}
	}

	// TODO Make the implementation
	static class ScoresCollector extends CollectorAbstract {

		private ScoresCollector(CollectorInterface parent) {
			super(parent);
		}

		@Override
		final public void collect(int docId) throws IOException {
			parent.collect(docId);
		}
	}

}
