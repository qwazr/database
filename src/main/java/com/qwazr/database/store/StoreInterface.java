/**
 * Copyright 2015 Emmanuel Keller / QWAZR
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
package com.qwazr.database.store;

import java.io.Closeable;

public interface StoreInterface extends Closeable {

	/**
	 * Get or create a new persistent map
	 *
	 * @param mapName the name of the map
	 * @param <K>     the type of the key of the map
	 * @param <V>     the type of the value of the map
	 * @return a persistent Map
	 */
	<K, V> StoreMapInterface<K, V> getMap(String mapName, ByteConverter<K> keyConverter,
										  ByteConverter<V> valueConverter);

	<T> SequenceInterface<T> getSequence(String sequenceName, Class<T> clazz);

	void deleteCollection(String collectionName);

	boolean exists(String collectionName);

}
