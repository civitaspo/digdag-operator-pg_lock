0.0.3 (2019-11-29)
==================

* [Fix] Fix the way to get `pg_lock.digdag.headers.*` values.

0.0.2 (2019-11-27)
==================

* [Enhancement] Add `unlock_finished_attempt_locks` option.

0.0.1 (2019-11-25)
==================

* [Enhancement] Release locks if owner_attempt is finished.
* [Enhancement] Use Github Actions instead of Circle CI.

0.0.1.pre3 (2019-10-14)
=======================

* [Fix] Fix the connection flood.

0.0.1.pre2 (2019-10-07)
=======================

* [Fix] Fix `org.flywaydb.core.api.FlywayException: Found non-empty schema(s) "public" without schema history table! Use baseline() or set baselineOnMigrate to true to initialize the schema history table.` when using this operator in digdag server mode. [#2](https://github.com/civitaspo/digdag-operator-pg_lock/pull/2)
* [Fix] Fix a namespace bug: `Unsupported namespace: session (config)` [#3](https://github.com/civitaspo/digdag-operator-pg_lock/pull/3)

0.0.1.pre1 (2019-10-05)
=======================

* First Release
