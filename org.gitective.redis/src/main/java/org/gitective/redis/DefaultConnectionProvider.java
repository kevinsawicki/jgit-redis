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

import redis.clients.jedis.Jedis;

/**
 * Default connection provider
 */
public class DefaultConnectionProvider implements ConnectionProvider {

	private final String host;

	/**
	 * Create connection provider bound to localhost
	 */
	public DefaultConnectionProvider() {
		this("localhost");
	}

	/**
	 * Create connection provider bound to given host
	 * 
	 * @param host
	 */
	public DefaultConnectionProvider(String host) {
		this.host = host;
	}

	public Jedis acquire() {
		return new Jedis(host);
	}

	public ConnectionProvider release(Jedis connection) {
		connection.disconnect();
		return this;
	}

}
