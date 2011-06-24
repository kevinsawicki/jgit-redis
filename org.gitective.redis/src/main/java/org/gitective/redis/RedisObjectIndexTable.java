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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.eclipse.jgit.generated.storage.dht.proto.GitStore;
import org.eclipse.jgit.storage.dht.AsyncCallback;
import org.eclipse.jgit.storage.dht.ChunkKey;
import org.eclipse.jgit.storage.dht.DhtException;
import org.eclipse.jgit.storage.dht.ObjectIndexKey;
import org.eclipse.jgit.storage.dht.ObjectInfo;
import org.eclipse.jgit.storage.dht.spi.Context;
import org.eclipse.jgit.storage.dht.spi.ObjectIndexTable;
import org.eclipse.jgit.storage.dht.spi.WriteBuffer;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Redis-backed object index table
 */
public class RedisObjectIndexTable extends RedisClient implements
		ObjectIndexTable {

	/**
	 */
	public RedisObjectIndexTable() {
		super();
	}

	/**
	 * @param provider
	 */
	public RedisObjectIndexTable(ConnectionProvider provider) {
		super(provider);
	}

	/**
	 * @param pool
	 */
	public RedisObjectIndexTable(JedisPool pool) {
		super(pool);
	}

	public void get(Context options, Set<ObjectIndexKey> objects,
			AsyncCallback<Map<ObjectIndexKey, Collection<ObjectInfo>>> callback) {
		Jedis jedis = acquire();
		try {
			Map<ObjectIndexKey, Collection<ObjectInfo>> out = new HashMap<ObjectIndexKey, Collection<ObjectInfo>>();
			for (ObjectIndexKey objId : objects) {
				Map<byte[], byte[]> values = jedis.hgetAll(objId.asBytes());
				for (Entry<byte[], byte[]> value : values.entrySet()) {
					Collection<ObjectInfo> chunks = out.get(objId);
					if (chunks == null) {
						chunks = new ArrayList<ObjectInfo>(4);
						out.put(objId, chunks);
					}
					chunks.add(new ObjectInfo(
							ChunkKey.fromBytes(value.getKey()), 0,
							GitStore.ObjectInfo.parseFrom(value.getValue())));
				}
			}
			callback.onSuccess(out);
		} catch (InvalidProtocolBufferException e) {
			callback.onFailure(new DhtException(e));
			return;
		} finally {
			release(jedis);
		}
	}

	public void add(ObjectIndexKey objId, ObjectInfo info, WriteBuffer buffer)
			throws DhtException {
		Jedis jedis = acquire();
		try {
			jedis.hset(objId.asBytes(), info.getChunkKey().asBytes(), info
					.getData().toByteArray());
		} finally {
			release(jedis);
		}
	}

	public void remove(ObjectIndexKey objId, ChunkKey chunk, WriteBuffer buffer)
			throws DhtException {
		Jedis jedis = acquire();
		try {
			jedis.hdel(objId.asBytes(), chunk.asBytes());
		} finally {
			release(jedis);
		}
	}
}
