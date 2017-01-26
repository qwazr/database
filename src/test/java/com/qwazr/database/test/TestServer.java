/**
 * Copyright 2016 Emmanuel Keller / QWAZR
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
package com.qwazr.database.test;

import com.google.common.io.Files;
import com.qwazr.database.TableServer;
import com.qwazr.database.TableServiceInterface;
import com.qwazr.server.RemoteService;
import com.qwazr.utils.http.HttpClients;

import java.io.File;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

public class TestServer {

	private static final String BASE_URL = "http://localhost:9091";

	static synchronized void start() throws Exception {
		if (TableServer.getInstance() != null)
			throw new Exception("Instance already running");
		final File dataDir = Files.createTempDir();
		System.setProperty("QWAZR_DATA", dataDir.getAbsolutePath());
		System.setProperty("LISTEN_ADDR", "localhost");
		System.setProperty("PUBLIC_ADDR", "localhost");
		TableServer.main();
	}

	private volatile static TableServiceInterface CLIENT = null;

	static synchronized TableServiceInterface getRemoteClient() throws URISyntaxException {
		if (CLIENT != null)
			return CLIENT;
		CLIENT = TableServer.getInstance().getServiceBuilder().remote(RemoteService.of(BASE_URL).build());
		return CLIENT;
	}

	static synchronized TableServiceInterface getLocalClient() {
		return TableServer.getInstance().getServiceBuilder().local();
	}

	public static void shutdown() {
		TableServer.shutdown();
		HttpClients.CNX_MANAGER.closeIdleConnections(2, TimeUnit.MINUTES);
		HttpClients.CNX_MANAGER.closeExpiredConnections();
		HttpClients.UNSECURE_CNX_MANAGER.closeIdleConnections(2, TimeUnit.MINUTES);
		HttpClients.UNSECURE_CNX_MANAGER.closeExpiredConnections();
		CLIENT = null;
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
