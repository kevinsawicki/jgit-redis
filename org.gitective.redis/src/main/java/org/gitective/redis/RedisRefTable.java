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

import com.google.protobuf.InvalidProtocolBufferException;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;

import org.eclipse.jgit.generated.storage.dht.proto.GitStore.RefData;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.RefDataUtil;
import org.eclipse.jgit.storage.dht.RefKey;
import org.eclipse.jgit.storage.dht.RepositoryKey;
import org.eclipse.jgit.storage.dht.spi.Context;
import org.eclipse.jgit.storage.dht.spi.RefTable;
import org.eclipse.jgit.storage.dht.spi.util.ColumnMatcher;
import org.eclipse.jgit.util.RawParseUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Redis-backed ref table
 */
public class RedisRefTable extends RedisClient implements RefTable {

	private static final ColumnMatcher REFS = new ColumnMatcher("refs:");

	/**
	 * 
	 */
	public RedisRefTable() {
		super();
	}

	/**
	 * @param provider
	 */
	public RedisRefTable(ConnectionProvider provider) {
		super(provider);
	}

	/**
	 * @param pool
	 */
	public RedisRefTable(JedisPool pool) {
		super(pool);
	}

	public Map<RefKey, RefData> getAll(Context options, RepositoryKey repository)
			throws DhtException, TimeoutException {
		Jedis jedis = acquire();
		try {
			Map<byte[], byte[]> values = jedis.hgetAll(REFS.append(repository
					.asBytes()));
			Map<RefKey, RefData> out = new HashMap<RefKey, RefData>(
					values.size());
			for (Entry<byte[], byte[]> value : values.entrySet())
				out.put(RefKey.create(repository,
						RawParseUtils.decode(value.getKey())),
						RefData.parseFrom(value.getValue()));
			return out;
		} catch (InvalidProtocolBufferException e) {
			throw new DhtException(e);
		} finally {
			release(jedis);
		}
	}

	public boolean compareAndPut(RefKey refKey, RefData oldData, RefData newData)
			throws DhtException, TimeoutException {
		byte[] repo = refKey.getRepositoryKey().asBytes();
		byte[] ref = Constants.encode(refKey.getName());
		byte[] oldBytes = oldData != RefDataUtil.NONE ? oldData.toByteArray()
				: null;
		Jedis jedis = acquire();
		try {
			byte[] key = REFS.append(repo);
			byte[] currBytes = jedis.hget(key, ref);
			boolean changed = changed(oldBytes, currBytes);
			if (changed)
				jedis.hset(key, ref, newData.toByteArray());
			return changed;
		} finally {
			release(jedis);
		}
	}

	public boolean compareAndRemove(RefKey refKey, RefData oldData)
			throws DhtException, TimeoutException {
		byte[] repo = refKey.getRepositoryKey().asBytes();
		byte[] ref = Constants.encode(refKey.getName());
		byte[] oldBytes = oldData != RefDataUtil.NONE ? oldData.toByteArray()
				: null;
		Jedis jedis = acquire();
		try {
			byte[] key = REFS.append(repo);
			byte[] currBytes = jedis.hget(key, ref);
			boolean changed = changed(oldBytes, currBytes);
			if (changed)
				jedis.hdel(key, ref);
			return changed;
		} finally {
			release(jedis);
		}
	}
}
