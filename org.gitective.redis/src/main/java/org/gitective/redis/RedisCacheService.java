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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.jgit.storage.dht.AsyncCallback;
import org.eclipse.jgit.storage.dht.spi.cache.CacheKey;
import org.eclipse.jgit.storage.dht.spi.cache.CacheService;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Redis-backed cache service
 */
public class RedisCacheService extends RedisClient implements CacheService {

	/**
	 */
	public RedisCacheService() {
		super();
	}

	/**
	 * @param provider
	 */
	public RedisCacheService(ConnectionProvider provider) {
		super(provider);
	}

	/**
	 * @param pool
	 */
	public RedisCacheService(JedisPool pool) {
		super(pool);
	}

	public void get(final Collection<CacheKey> keys,
			final AsyncCallback<Map<CacheKey, byte[]>> callback) {
		Jedis connection = acquire();
		try {
			Map<CacheKey, byte[]> cached = new HashMap<CacheKey, byte[]>();
			for (CacheKey key : keys) {
				byte[] value = connection.get(key.getBytes());
				if (value != null)
					cached.put(key, value);
			}
			callback.onSuccess(cached);
		} finally {
			release(connection);
		}
	}

	public void modify(final Collection<Change> changes,
			final AsyncCallback<Void> callback) {
		Jedis connection = acquire();
		try {
			for (Change change : changes)
				connection.set(change.getKey().getBytes(), change.getData());
			callback.onSuccess(null);
		} finally {
			release(connection);
		}
	}
}
