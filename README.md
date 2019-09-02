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
  lock_wait_timeout: 5m
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
    max_count: 5
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
- **pg_lock.user**: The user name of PostgreSQL. (string, required)
- **pg_lock.password**: The password of PostgreSQL. (string, optional)
- **pg_lock.poll_interval**: The polling interval to wait for getting the named lock. (`DurationParam`, default: `5s`)
- **pg_lock.min_poll_interval**: The minimum polling interval to wait for getting the named lock. (`DurationParam`, default: `5s`)
- **pg_lock.max_poll_interval**: The maximum polling interval to wait for getting the named lock. (`DurationParam`, default: `5m`)

## Operator configurations

- **pg_lock>**: The name of lock. This name is used across workflows and projects. (string, required)
- **wait_timeout**: The timeout to wait for getting the named lock. (`DurationParam`, default: `15m`)
- **expire_in**: The duration that the named lock expires in. (`DurationParam`, default: `"1h"`)
- **max_count**: The maximum count of the named locks within the namespace. If the different value is defined in another task, throw `ConfigException`. (integer, default: `1`) 
- **namespace**: The namespace that the named lock can be unique. The valid values are `"site"`, `"project"`, `"workflow"`, `"session"`, and`"attempt"`. (string, default: `"site"`)
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

