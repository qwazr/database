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

import com.googlecode.concurrenttrees.radix.node.Node;
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharArrayNodeFactory;

import java.util.HashMap;
import java.util.List;


public class StoreNodeFactory extends DefaultCharArrayNodeFactory {

	private HashMap<CharSequence, Node> nodes = new HashMap<CharSequence, Node>();
	private int count = 0;

	@Override
	public Node createNode(CharSequence edgeCharacters, Object value, List<Node> childNodes, boolean isRoot) {
		Node node = super.createNode(edgeCharacters, value, childNodes, isRoot);
		nodes.put(edgeCharacters, node);
		count++;
		System.out.println(this.hashCode() + " : " + nodes.size() + " " + count);
		return node;
	}
}
