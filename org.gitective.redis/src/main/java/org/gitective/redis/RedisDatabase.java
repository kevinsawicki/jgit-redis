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

import java.io.IOException;

import org.eclipse.jgit.storage.dht.DhtRepository;
import org.eclipse.jgit.storage.dht.DhtRepositoryBuilder;
import org.eclipse.jgit.storage.dht.spi.ChunkTable;
import org.eclipse.jgit.storage.dht.spi.Database;
import org.eclipse.jgit.storage.dht.spi.ObjectIndexTable;
import org.eclipse.jgit.storage.dht.spi.RefTable;
import org.eclipse.jgit.storage.dht.spi.RepositoryIndexTable;
import org.eclipse.jgit.storage.dht.spi.RepositoryTable;
import org.eclipse.jgit.storage.dht.spi.WriteBuffer;
import org.eclipse.jgit.util.FS;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol;

/**
 * Redis-backed database
 */
public class RedisDatabase implements Database {

	private final RedisRepositoryIndexTable repositoryIndex;

	private final RedisRepositoryTable repository;

	private final RedisRefTable ref;

	private final RedisObjectIndexTable objectIndex;

	private final RedisChunkTable chunk;

	/**
	 * Create redis-backed database
	 */
	public RedisDatabase() {
		PoolConnectionProvider provider = new PoolConnectionProvider(new JedisPool("localhost",
				Protocol.DEFAULT_PORT));
		repositoryIndex = new RedisRepositoryIndexTable(provider);
		repository = new RedisRepositoryTable(provider);
		ref = new RedisRefTable(provider);
		objectIndex = new RedisObjectIndexTable(provider);
		chunk = new RedisChunkTable(provider);
	}

	/**
	 * Open repository on this database
	 * 
	 * @param name
	 * @return repository
	 * @throws IOException
	 */
	public DhtRepository open(String name) throws IOException {
		return (DhtRepository) new DhtRepositoryBuilder<DhtRepositoryBuilder, DhtRepository, RedisDatabase>()
				.setDatabase(this).setRepositoryName(name).setMustExist(false)
				.setFS(FS.DETECTED).build();
	}

	public RepositoryIndexTable repositoryIndex() {
		return repositoryIndex;
	}

	public RepositoryTable repository() {
		return repository;
	}

	public RefTable ref() {
		return ref;
	}

	public ObjectIndexTable objectIndex() {
		return objectIndex;
	}

	public ChunkTable chunk() {
		return chunk;
	}

	public WriteBuffer newWriteBuffer() {
		return new RedisWriteBuffer();
	}
}
