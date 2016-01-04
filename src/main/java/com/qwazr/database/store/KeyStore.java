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
 **/
package com.qwazr.database.store;

import com.qwazr.utils.LockUtils;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KeyStore implements Closeable {

    public static final int MAX_WRITE_CACHE_SIZE = 50000;

    private static final Logger logger = LoggerFactory.getLogger(KeyStore.class);

    private final DB db;

    private static final byte[] deletedKey = new byte[0];

    private final LockUtils.ReadWriteLock rwlMemory = new LockUtils.ReadWriteLock();
    private final ConcurrentHashMap<byte[], byte[]> memoryTemp = new ConcurrentHashMap<byte[], byte[]>();

    public KeyStore(File file) throws IOException {
	Options options = new Options();
	options.createIfMissing(true);
	db = JniDBFactory.factory.open(file, options);
    }

    public void commit() throws IOException {
	rwlMemory.r.lock();
	try {
	    if (logger.isDebugEnabled())
		logger.debug("Write batch: " + memoryTemp.size());
	    WriteBatch batch = db.createWriteBatch();
	    try {
		memoryTemp.forEach((key, value) -> {
		    if (key == deletedKey)
			batch.delete(key);
		    else
			batch.put(key, value);
		});
		memoryTemp.clear();
		db.write(batch);
	    } finally {
		batch.close();
	    }
	} finally {
	    rwlMemory.r.unlock();
	}
    }

    @Override
    public void close() throws IOException {
	commit();
	db.close();
    }

    public byte[] get(byte[] key) {
	rwlMemory.r.lock();
	try {
	    byte[] value = memoryTemp.get(key);
	    if (value == deletedKey)
		return null;
	    if (value != null)
		return value;
	    return db.get(key);
	} finally {
	    rwlMemory.r.unlock();
	}
    }

    public void put(byte[] key, byte[] value) throws IOException {
	rwlMemory.w.lock();
	try {
	    if (value == null || value.length == 0)
		value = deletedKey;
	    memoryTemp.put(key, value);
	} finally {
	    rwlMemory.w.unlock();
	}
	if (memoryTemp.size() >= MAX_WRITE_CACHE_SIZE)
	    commit();
    }

    public void delete(byte[] key) throws IOException {
	rwlMemory.w.lock();
	try {
	    memoryTemp.put(key, deletedKey);
	} finally {
	    rwlMemory.w.unlock();
	}
	if (memoryTemp.size() >= MAX_WRITE_CACHE_SIZE)
	    commit();
    }

    public DBIterator iterator() {
	return db.iterator();
    }
}
