/**
 * Copyright 2015-2016 Emmanuel Keller / QWAZR
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
package com.qwazr.database;

import com.qwazr.database.store.KeyStore;
import org.apache.commons.lang3.SystemUtils;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.net.URISyntaxException;

@RunWith(Suite.class)
@Suite.SuiteClasses({ TestSuite.LevelDbLocalTest.class, TestSuite.LevelDbRemoteTest.class, TestSuite.LmdbTest.class })
public class TestSuite {

	public static class LevelDbLocalTest extends JsonTest {

		@Override
		protected KeyStore.Impl getStoreImplementation() {
			return SystemUtils.IS_OS_WINDOWS ? KeyStore.Impl.lmdb : KeyStore.Impl.leveldb;
		}

		@Override
		protected TableServiceInterface getClient() {
			return TestServer.getLocalClient();
		}
	}

	public static class LevelDbRemoteTest extends JsonTest {

		@Override
		protected KeyStore.Impl getStoreImplementation() {
			return SystemUtils.IS_OS_WINDOWS ? KeyStore.Impl.lmdb : KeyStore.Impl.leveldb;
		}

		@Override
		protected TableServiceInterface getClient() throws URISyntaxException {
			return TestServer.getRemoteClient();
		}
	}

	public static class LmdbTest extends JsonTest {

		@Override
		protected KeyStore.Impl getStoreImplementation() {
			return KeyStore.Impl.lmdb;
		}

		@Override
		protected TableServiceInterface getClient() throws URISyntaxException {
			return TestServer.getRemoteClient();
		}
	}

}
