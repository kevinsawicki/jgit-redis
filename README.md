# JGit Redis connector

This project is a [JGit](http://www.eclipse.org/jgit/) DHT implementation using
[Redis](http://redis.io/) as the backing database that uses the [Jedis](https://github.com/xetorthio/jedis)
library for connecting to Redis.

## Example

The code snippet below shows how to fetch the linux-2.6 Git repository into
Redis using a JGit repository.

```java
Repository repo = new RedisDatabase().open("linux-2.6");
repo.create(true);
StoredConfig config = repo.getConfig();
RemoteConfig remoteConfig = new RemoteConfig(config, "origin");
URIish uri = new URIish("git://github.com/mirrors/linux-2.6.git");
remoteConfig.addURI(uri);
remoteConfig.update(config);
config.save();
RefSpec spec = new RefSpec("refs/heads/*:refs/remotes/origin/*");
Git.wrap(repo).fetch().setRemote("origin").setRefSpecs(spec).call();
```
