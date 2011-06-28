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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.eclipse.jgit.generated.storage.dht.proto.GitStore.CachedPackInfo;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.storage.dht.CachedPackKey;
import org.eclipse.jgit.storage.dht.ChunkInfo;
import org.eclipse.jgit.storage.dht.ChunkKey;
import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.RepositoryKey;
import org.eclipse.jgit.storage.dht.spi.RepositoryTable;
import org.eclipse.jgit.storage.dht.spi.WriteBuffer;
import org.eclipse.jgit.storage.dht.spi.util.ColumnMatcher;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Redis-backed repository table
 */
public class RedisRepositoryTable extends RedisClient implements
		RepositoryTable {

	private static final ColumnMatcher REPO_TABLE = new ColumnMatcher("rt:");

	private static final ColumnMatcher CACHE_PACK = new ColumnMatcher(
			REPO_TABLE.toString() + "chce:");

	private static final byte[] KEY_SUFFIX = Constants.encode("key");

	/**
	 */
	public RedisRepositoryTable() {
		super();
	}

	/**
	 * @param provider
	 */
	public RedisRepositoryTable(ConnectionProvider provider) {
		super(provider);
	}

	/**
	 * @param pool
	 */
	public RedisRepositoryTable(JedisPool pool) {
		super(pool);
	}

	public RepositoryKey nextKey() throws DhtException {
		Jedis connection = acquire();
		try {
			return RepositoryKey.fromInt(connection.incr(
					REPO_TABLE.append(KEY_SUFFIX)).intValue());
		} finally {
			release(connection);
		}
	}

	public void put(RepositoryKey repo, ChunkInfo info, WriteBuffer buffer)
			throws DhtException {
		Jedis connection = acquire();
		try {
			connection.hset(repo.asBytes(), info.getChunkKey().asBytes(), info
					.getData().toByteArray());
		} finally {
			release(connection);
		}
	}

	public void remove(RepositoryKey repo, ChunkKey chunk, WriteBuffer buffer)
			throws DhtException {
		Jedis connection = acquire();
		try {
			connection.hdel(repo.asBytes(), chunk.asBytes());
		} finally {
			release(connection);
		}
	}

	public Collection<CachedPackInfo> getCachedPacks(RepositoryKey repo)
			throws DhtException, TimeoutException {
		Jedis connection = acquire();
		try {
			Collection<byte[]> values = connection.hgetAll(
					CACHE_PACK.append(repo.asBytes())).values();
			List<CachedPackInfo> out = new ArrayList<CachedPackInfo>(
					values.size());
			for (byte[] value : values)
				out.add(CachedPackInfo.parseFrom(value));
			return out;
		} catch (InvalidProtocolBufferException e) {
			throw new DhtException(e);
		} finally {
			release(connection);
		}
	}

	public void put(RepositoryKey repo, CachedPackInfo info, WriteBuffer buffer)
			throws DhtException {
		CachedPackKey key = CachedPackKey.fromInfo(info);
		Jedis connection = acquire();
		try {
			connection.hset(CACHE_PACK.append(repo.asBytes()), key.asBytes(),
					info.toByteArray());
		} finally {
			release(connection);
		}
	}

	public void remove(RepositoryKey repo, CachedPackKey key, WriteBuffer buffer)
			throws DhtException {
		Jedis connection = acquire();
		try {
			connection.hdel(CACHE_PACK.append(repo.asBytes()), key.asBytes());
		} finally {
			release(connection);
		}
	}
}
