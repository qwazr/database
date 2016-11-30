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
import com.qwazr.utils.server.GenericServer;
import com.qwazr.utils.server.RemoteService;

import java.io.File;
import java.net.URISyntaxException;

/**
 * Created by ekeller on 09/11/2016.
 */
public class TestServer {

	private static final String BASE_URL = "http://localhost:9091";

	private static GenericServer genericServer = null;

	static synchronized void start() throws Exception {
		if (genericServer != null)
			return;
		final File dataDir = Files.createTempDir();
		System.setProperty("QWAZR_DATA", dataDir.getAbsolutePath());
		System.setProperty("LISTEN_ADDR", "localhost");
		System.setProperty("PUBLIC_ADDR", "localhost");
		genericServer = TableServer.start();
	}

	private static TableServiceInterface CLIENT = null;

	static synchronized TableServiceInterface getClient() throws URISyntaxException {
		if (CLIENT != null)
			return CLIENT;
		CLIENT = TableServiceInterface.getClient(new RemoteService(BASE_URL));
		return CLIENT;
	}

}
