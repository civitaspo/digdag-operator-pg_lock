# digdag-operator-pg_lock
[![Jitpack](https://jitpack.io/v/pro.civitaspo/digdag-operator-operator-pg_lock.svg)](https://jitpack.io/#pro.civitaspo/digdag-operator-pg_lock) [![CircleCI](https://circleci.com/gh/civitaspo/digdag-operator-pg_lock.svg?style=shield)](https://circleci.com/gh/civitaspo/digdag-operator-pg_lock) [![Digdag](https://img.shields.io/badge/digdag-v0.9.39-brightgreen.svg)](https://github.com/treasure-data/digdag/releases/tag/v0.9.39)

A digdag plugin to run digdag tasks with locks by PostgreSQL.

# Overview

- Plugin type: operator

# Usage

```yaml
_export:
  plugin:
    repositories:
      - https://jitpack.io
    dependencies:
      - pro.civitaspo:digdag-operator-pg_lock:0.0.1

+lock-with:
  # Wait during 5m until getting the named lock if another task locks.
  # This named lock can be used beyond the workflow and the project,
  # that means if another task in another project locks by the name, this task waits until the lock is released.
  pg_lock>: lock-1
  wait_timeout: 5m
  _do:
    echo>: hello world

+control-parallelism:
  # Tasks can get 5 locks.
  # When parallel tasks run with this feature, you can control the parallelism.
  # The below example means the tasks run in 5 parallels.
  loop>: 100
  _parallel: true
  _do:
    pg_lock>: lock-2  
    limit: 5
    _do:
      echo>: hello world
    
```

See [example](./example).

# Configuration

## Remarks

- type `DurationParam` is strings matched `\s*(?:(?<days>\d+)\s*d)?\s*(?:(?<hours>\d+)\s*h)?\s*(?:(?<minutes>\d+)\s*m)?\s*(?:(?<seconds>\d+)\s*
s)?\s*`.
  - The strings is used as `java.time.Duration`.
  
## System configurations

- **pg_lock.host**: The host name of PostgreSQL. (string, required)
- **pg_lock.port**: The port of PostgreSQL. (integer, default: `5432`)
- **pg_lock.database**: The database name of PostgreSQL. (string, required)
- **pg_lock.schemata**: The schema (or several schemata separated by commas) to be set in the search-path. (string, default: `"public"`)
- **pg_lock.user**: The user name of PostgreSQL. (string, required)
- **pg_lock.password**: The password of PostgreSQL. (string, optional)
- **pg_lock.login_timeout**: The timeout duration to wait for establishment of a PostgreSQL database connection. (`DurationParam`, default: `30s`)
- **pg_lock.socket_timeout**: The timeout duration for socket read operations. If reading from the server takes longer than this value, the connection is closed. This can be used as both a brute force global query timeout and a method of detecting network problems. `0s` means that it is disabled. (`DurationParam`, default: `30m`)
- **pg_lock.ssl**: (boolean, default: `false`)
- **pg_lock.connection_timeout**: The timeout duration that a client will wait for a connection from the pool. If this time is exceeded without a connection becoming available, a `SQLException` will be thrown. (`DurationParam`, default: `30s`)
- **pg_lock.idle_timeout**: The timeout duration that a connection is allowed to sit idle in the pool. `0s` means that idle connections are never removed from the pool. (`DurationParam`, default: `10m`)
- **pg_lock.validation_timeout**: The timeout duration that the pool will wait for a connection to be validated as alive. (`DurationParam`, default: `5s`)
- **pg_lock.max_pool_size**: The connection pool size that is allowed to reach, including both idle and in-use connections. (integer, default: `5`)
- **pg_lock.min_idle_size**: The property controls the minimum number of idle connections in the pool. (integer, default: the same value as **pg_lock.max_pool_size**)
- **pg_lock.max_life_time**: This property controls the maximum lifetime of a connection in the pool. When a connection reaches this timeout, even if recently used, it will be retired from the pool. An in-use connection will never be retired, only when it is idle will it be removed. (`DurationParam`, default: `30m`)
- **pg_lock.leak_detection_threshold**: The threshold that a connection can be out of the pool before a message is logged indicating a possible connection leak. `0s` means that it is disabled. (`DurationParam`, default: `0s`)
- **pg_lock.poll_interval**: The polling interval to wait for getting the named lock. (`DurationParam`, default: `5s`)
- **pg_lock.min_poll_interval**: The minimum polling interval to wait for getting the named lock. (`DurationParam`, default: `5s`)
- **pg_lock.max_poll_interval**: The maximum polling interval to wait for getting the named lock. (`DurationParam`, default: `5m`)
- **pg_lock.schema_migration**: Whether do schema migration or not. (boolean, default: `true`)
- **pg_loch.schema_migration_history_table**: The table name to write schema migration history. (string, default: `"pg_lock_schema_migrations"`)
- **pg_lock.hash_seed_for_advisory_lock**: The seed to hash strings with "MurmurHash 3" algorithm for `pg_try_advisory_lock`. (integer, default: `-137723950` (the same as `scala.util.hashing.MurmurHash3.stringSeed`))


## Operator configurations

- **pg_lock>**: The name of lock. This name is used across workflows and projects. (string, required)
- **wait_timeout**: The timeout to wait for getting the named lock. (`DurationParam`, default: `15m`)
- **expire_in**: The duration that the named lock expires in. (`DurationParam`, default: `"1h"`)
- **limit**: The limit count of the named locks within the namespace. If the different value is defined in another task, throw `ConfigException`. (integer, default: `1`) 
- **namespace**: The namespace that the named lock can be unique. The valid values are `"global"`, `"site"`, `"project"`, `"workflow"`, `"session"`, and`"attempt"`. (string, default: `"site"`)
- **_do**: The definition of subtasks with the named lock. (config, required) 

# Development

## Run an Example

### 1) build

```sh
./gradlew publish
```

Artifacts are build on local repos: `./build/repo`.

### 2) get your aws profile

```sh
aws configure
```

### 3) run an example

```sh
## Run PostgreSQL foreground.
./example/run-pg.sh

## Run examples
./example/run.sh
```

## Run Tests

```sh
./gradlew test
```

# ChangeLog

[CHANGELOG.md](./CHANGELOG.md)

# License

[Apache License 2.0](./LICENSE.txt)

# Author

@civitaspo

