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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.mapdb.DB;

public class StoredField extends FieldAbstract {

	private final Map<Integer, String> map;
	private final String collectionName;

	public StoredField(String name, long fieldId, DB storeDb,
			AtomicBoolean wasExisting) {
		super(name, fieldId);
		collectionName = "store." + fieldId;
		wasExisting.set(storeDb.exists(collectionName));
		map = storeDb.getTreeMap(collectionName);
	}

	@Override
	public void deleteDocument(Integer id) {
		map.remove(id);
	}

	@Override
	public void setValue(Integer id, String value) {
		map.put(id, value);
	}

	@Override
	public void setValues(Integer docId, Collection<String> values) {
		throw new IllegalArgumentException(
				"Only one value allowed for this field: " + this.name);
	}

	@Override
	public List<String> getValues(Integer id) {
		String value = map.get(id);
		if (value == null)
			return Collections.emptyList();
		ArrayList<String> list = new ArrayList<String>(1);
		list.add(value);
		return list;
	}
}
