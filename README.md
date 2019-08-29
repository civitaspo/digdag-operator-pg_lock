# digdag-plugin-scala-template
[![Jitpack](https://jitpack.io/v/pro.civitaspo/digdag-operator-plugin-scala-template.svg)](https://jitpack.io/#pro.civitaspo/digdag-operator-plugin-scala-template) [![CircleCI](https://circleci.com/gh/civitaspo/digdag-operator-plugin-scala-template.svg?style=shield)](https://circleci.com/gh/civitaspo/digdag-operator-plugin-scala-template) [![Digdag](https://img.shields.io/badge/digdag-v0.9.39-brightgreen.svg)](https://github.com/treasure-data/digdag/releases/tag/v0.9.39)

This is a digdag plugin template for scala implementations.

# Overview

- Plugin type: operator

# Usage

```yaml
_export:
  plugin:
    repositories:
      - https://jitpack.io
    dependencies:
      - pro.civitaspo:digdag-plugin-scala-template:0.0.1

+step1:
  example>: template.txt
  message: yes
  path: example.out

+step2:
  hello>: "hello "
  message: world
```

See [example](./example).

# Configuration

TODO

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

## (TODO) Run Tests

```sh
./gradlew test
```

# ChangeLog

[CHANGELOG.md](./CHANGELOG.md)

# License

[Apache License 2.0](./LICENSE.txt)

# Author

@civitaspo

