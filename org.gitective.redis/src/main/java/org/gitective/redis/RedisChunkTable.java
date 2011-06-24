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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.jgit.generated.storage.dht.proto.GitStore.ChunkMeta;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.storage.dht.AsyncCallback;
import org.eclipse.jgit.storage.dht.ChunkKey;
import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.PackChunk;
import org.eclipse.jgit.storage.dht.PackChunk.Members;
import org.eclipse.jgit.storage.dht.spi.ChunkTable;
import org.eclipse.jgit.storage.dht.spi.Context;
import org.eclipse.jgit.storage.dht.spi.WriteBuffer;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Redis-backed chunk table
 */
public class RedisChunkTable extends RedisClient implements ChunkTable {

	private static final byte[] DATA = Constants.encode("data");

	private static final byte[] INDEX = Constants.encode("index");

	private static final byte[] META = Constants.encode("meta");

	/**
	 * 
	 */
	public RedisChunkTable() {
		super();
	}

	/**
	 * @param provider
	 */
	public RedisChunkTable(ConnectionProvider provider) {
		super(provider);
	}

	/**
	 * @param pool
	 */
	public RedisChunkTable(JedisPool pool) {
		super(pool);
	}

	public void get(Context options, Set<ChunkKey> keys,
			AsyncCallback<Collection<Members>> callback) {
		List<PackChunk.Members> out = new ArrayList<PackChunk.Members>(
				keys.size());
		Jedis jedis = acquire();
		try {
			for (ChunkKey chunk : keys) {
				byte[] row = chunk.asBytes();
				byte[] buffer = jedis.hget(row, DATA);
				if (buffer == null)
					continue;

				PackChunk.Members members = new PackChunk.Members();
				members.setChunkKey(chunk);
				members.setChunkData(buffer);

				buffer = jedis.hget(row, INDEX);
				if (buffer != null)
					members.setChunkIndex(buffer);

				buffer = jedis.hget(row, META);
				if (buffer != null)
					try {
						members.setMeta(ChunkMeta.parseFrom(buffer));
					} catch (InvalidProtocolBufferException e) {
						callback.onFailure(new DhtException(e));
						return;
					}
				out.add(members);
			}
			callback.onSuccess(out);
		} finally {
			release(jedis);
		}
	}

	public void getMeta(Context options, Set<ChunkKey> keys,
			AsyncCallback<Map<ChunkKey, ChunkMeta>> callback) {
		Map<ChunkKey, ChunkMeta> out = new HashMap<ChunkKey, ChunkMeta>();
		Jedis jedis = acquire();
		try {
			for (ChunkKey chunk : keys) {
				byte[] value = jedis.hget(chunk.asBytes(), META);
				if (value != null)
					out.put(chunk, ChunkMeta.parseFrom(value));
			}
			callback.onSuccess(out);
		} catch (InvalidProtocolBufferException e) {
			return;
		} finally {
			release(jedis);
		}
	}

	public void put(Members chunk, WriteBuffer buffer) throws DhtException {
		Jedis jedis = acquire();
		try {
			final byte[] row = chunk.getChunkKey().asBytes();

			if (chunk.hasChunkData())
				jedis.hset(row, DATA, chunk.getChunkData());

			if (chunk.hasChunkIndex())
				jedis.hset(row, INDEX, chunk.getChunkIndex());

			if (chunk.hasMeta())
				jedis.hset(row, META, chunk.getMeta().toByteArray());

		} finally {
			release(jedis);
		}
	}

	public void remove(ChunkKey key, WriteBuffer buffer) throws DhtException {
		Jedis jedis = acquire();
		try {
			jedis.del(key.asBytes());
		} finally {
			release(jedis);
		}
	}
}
