0.0.1.pre3 (2019-10-14)
=======================

* Fix the connection flood.

0.0.1.pre2 (2019-10-07)
=======================

* Fix `org.flywaydb.core.api.FlywayException: Found non-empty schema(s) "public" without schema history table! Use baseline() or set baselineOnMigrate to true to initialize the schema history table.` when using this operator in digdag server mode. [#2](https://github.com/civitaspo/digdag-operator-pg_lock/pull/2)
* Fix a namespace bug: `Unsupported namespace: session (config)` [#3](https://github.com/civitaspo/digdag-operator-pg_lock/pull/3)

0.0.1.pre1 (2019-10-05)
=======================

* First Release
