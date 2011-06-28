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

import java.util.Arrays;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Base client class
 */
public class RedisClient {

	/**
	 * Are the two given byte arrays equal?
	 * 
	 * @param old
	 * @param current
	 * @return true if equal, false if different
	 */
	public static boolean equal(byte[] old, byte[] current) {
		return (old == null && current == null) || Arrays.equals(old, current);
	}

	private final ConnectionProvider provider;

	/**
	 * Create redis client using default connection provider
	 */
	public RedisClient() {
		this(new DefaultConnectionProvider());
	}

	/**
	 * Create redis client using given pool
	 * 
	 * @param pool
	 */
	public RedisClient(JedisPool pool) {
		this(new PoolConnectionProvider(pool));
	}

	/**
	 * Create redis client using given connection provider
	 * 
	 * @param provider
	 */
	public RedisClient(ConnectionProvider provider) {
		this.provider = provider;
	}

	/**
	 * Acquire connection
	 * 
	 * @return connection
	 */
	protected Jedis acquire() {
		return provider.acquire();
	}

	/**
	 * Release connection
	 * 
	 * @param connection
	 * @return this client
	 */
	protected RedisClient release(Jedis connection) {
		provider.release(connection);
		return this;
	}

}
