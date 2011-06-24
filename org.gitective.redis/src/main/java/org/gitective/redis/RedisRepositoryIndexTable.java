/*
 * Copyright (c) 2011 Kevin Sawicki <kevinsawicki@gmail.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */
package org.gitective.redis;

import java.util.concurrent.TimeoutException;

import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.RepositoryKey;
import org.eclipse.jgit.storage.dht.RepositoryName;
import org.eclipse.jgit.storage.dht.spi.RepositoryIndexTable;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Redis-backed repository index table
 */
public class RedisRepositoryIndexTable extends RedisClient implements
		RepositoryIndexTable {

	private static final String REPOS = "repos";

	/**
	 * 
	 */
	public RedisRepositoryIndexTable() {
		super();
	}

	/**
	 * @param provider
	 */
	public RedisRepositoryIndexTable(ConnectionProvider provider) {
		super(provider);
	}

	/**
	 * @param pool
	 */
	public RedisRepositoryIndexTable(JedisPool pool) {
		super(pool);
	}

	public RepositoryKey get(RepositoryName name) throws DhtException,
			TimeoutException {
		Jedis connection = acquire();
		try {
			String key = connection.hget(REPOS, name.asString());
			return key != null ? RepositoryKey.fromString(key) : null;
		} finally {
			release(connection);
		}
	}

	public void putUnique(RepositoryName name, RepositoryKey key)
			throws DhtException, TimeoutException {
		Jedis connection = acquire();
		try {
			connection.hset(REPOS, name.asString(), key.asString());
		} finally {
			release(connection);
		}
	}

	public void remove(RepositoryName name, RepositoryKey key)
			throws DhtException, TimeoutException {
		Jedis connection = acquire();
		try {
			String nameKey = name.asString();
			String value = connection.hget(REPOS, nameKey);
			if (key.asString().equals(value))
				connection.hdel(REPOS, nameKey);
		} finally {
			release(connection);
		}
	}
}
