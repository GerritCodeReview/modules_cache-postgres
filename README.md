# Gerrit Postgres based persistent cache

Gerrit lib module to swap existing persistent cache implementation
(that is H2 based) with implementation that stores data in Postgres.
Note that is uses [INSERT INTO ... ON CONFLICT DO UPDATE ...](https://stackoverflow.com/questions/17267417/how-to-upsert-merge-insert-on-duplicate-update-in-postgresql)
upsert syntax therefore it is Postgres *9.5+* compatible.

## How to build

Build this module similarly to standalone build for any other,
bazel based Gerrit plugin:

- Clone the cache-postgres source tree
- Run ```bazel build cache-postgres```
- The ```cache-postgres.jar``` module is generated under ```/bazel-genfiles/cache-postgres.jar```

## How install

Copy ```cache-postgres.jar``` library to Gerrit ```/lib``` and add the following
two extra settings to ```gerrit.config```:

```
[gerrit]
  installModule = com.googlesource.gerrit.modules.cache.pg.CacheModule

[cache]
  url = jdbc:postgresql://localhost:5432/gerrit_caches?user=gerrit&password=gerrit
```

## Core Gerrit settings: section `cache`

cache.url
: URI that specifies connection to existing DB (including both
username and passwword).

cache.poolLimit
: Maximum number of open database connections. If the server needs
more than this number, request processing threads will wait up
to `cache.poolMaxWait` seconds for a connection to be released before
they abort with an exception.
Default value is taken from `database.poolLimit`.

cache.poolMinIdle
: Minimum number of connections to keep idle in the pool.
Default is `4`.

cache.poolMaxIdle
: Maximum number of connections to keep idle in the pool. If there
are more idle connections, connections will be closed instead of
being returned back to the pool.
Default is min(`cache.poolLimit`, `16`).

cache.poolMaxWait
Maximum amount of time a request processing thread will wait to
acquire a database connection from the pool. If no connection is
released within this time period, the processing thread will abort
its current operations and return an error to the client.
Values should use common unit suffixes to express their setting:
* ms, milliseconds
* s, sec, second, seconds
* m, min, minute, minutes
* h, hr, hour, hours

If a unit suffix is not specified, `milliseconds` is assumed.
Default is `30 seconds`.
