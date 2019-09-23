package pro.civitaspo.digdag.plugin.pg_lock.pg


import java.util.{Properties => JProperties}

import io.digdag.client.config.Config
import io.digdag.util.DurationParam

import scala.jdk.CollectionConverters._
import scala.util.chaining._


case class PgLockPgConfig(
    host: String,
    port: Int,
    database: String,
    user: String,
    password: Option[String],
    loginTimeout: DurationParam,
    socketTimeout: DurationParam,
    ssl: Boolean,
    connectionTimeout: DurationParam,
    idleTimeout: DurationParam,
    validationTimeout: DurationParam,
    maxPoolSize: Int,
    minIdleSize: Int,
    maxLifeTime: DurationParam,
    leakDetectionThreshold: DurationParam,
    schemaMigration: Boolean,
    schemaMigrationHistoryTable: String,
    opts: Map[String, String],
    driverClassName: String = "org.postgresql.Driver",
)
{
    val jdbcUrl: String = f"jdbc:postgresql://$host:$port/$database"
    val jdbcProperties: JProperties = {
        new JProperties().tap { props =>
            props.setProperty("loginTimeout",
                              loginTimeout.getDuration.getSeconds.toString
                              )
            props.setProperty("socketTimeout",
                              socketTimeout.getDuration.getSeconds.toString
                              )
            props.setProperty("tcpKeepAlive", "true")
            props.setProperty("user", user)
            password.foreach(pw => props.setProperty("password", pw))
            if (ssl) {
                props.setProperty("ssl", "true")
                // disable server certificate validation
                props.setProperty("sslfactory", "org.postgresql.ssl.NonValidatingFactory")
            }
            opts.foreach {
                case (k: String, v: String) => props.setProperty(k, v)
            }
        }
    }
}

object PgLockPgConfig
{
    val CONFIG_KEY_PREFIX: String = "pg_lock"

    def apply(systemConfig: Config): PgLockPgConfig =
    {
        PgLockPgConfig(
            host = systemConfig.get(s"$CONFIG_KEY_PREFIX.host", classOf[String]),
            port = systemConfig.get(s"$CONFIG_KEY_PREFIX.port", classOf[Int], 5432),
            database = systemConfig.get(s"$CONFIG_KEY_PREFIX.database", classOf[String]),
            user = systemConfig.get(s"$CONFIG_KEY_PREFIX.user", classOf[String]),
            password = Option(systemConfig.getOptional(s"$CONFIG_KEY_PREFIX.password", classOf[String]).orNull()),
            loginTimeout = systemConfig.get(s"$CONFIG_KEY_PREFIX.login_timeout", classOf[DurationParam], DurationParam.parse("30s")),
            socketTimeout = systemConfig.get(s"$CONFIG_KEY_PREFIX.socket_timeout", classOf[DurationParam], DurationParam.parse("30m")),
            ssl = systemConfig.get(s"$CONFIG_KEY_PREFIX.ssl", classOf[Boolean], false),
            connectionTimeout = systemConfig.get(s"$CONFIG_KEY_PREFIX.connection_timeout", classOf[DurationParam], DurationParam.parse("30s")),
            idleTimeout = systemConfig.get(s"$CONFIG_KEY_PREFIX.idle_timeout", classOf[DurationParam], DurationParam.parse("10m")),
            validationTimeout = systemConfig.get(s"$CONFIG_KEY_PREFIX.validation_timeout", classOf[DurationParam], DurationParam.parse("5s")),
            maxPoolSize = systemConfig.get(s"$CONFIG_KEY_PREFIX.max_pool_size", classOf[Int], 5),
            minIdleSize = systemConfig.get(s"$CONFIG_KEY_PREFIX.min_idle_size", classOf[Int], 5), // the default value must be the same value as maxPoolSize.
            maxLifeTime = systemConfig.get(s"$CONFIG_KEY_PREFIX.max_life_time", classOf[DurationParam], DurationParam.parse("30m")),
            leakDetectionThreshold = systemConfig.get(s"$CONFIG_KEY_PREFIX.leak_detection_threshold", classOf[DurationParam], DurationParam.parse("0s")),
            schemaMigration = systemConfig.get(s"$CONFIG_KEY_PREFIX.schema_migration", classOf[Boolean], true),
            schemaMigrationHistoryTable = systemConfig.get(s"$CONFIG_KEY_PREFIX.schema_migration_history_table", classOf[String], "pg_lock_schema_migrations"),
            opts = extractOpts(systemConfig)
            )
    }

    private def extractOpts(systemConfig: Config): Map[String, String] =
    {
        val optsKeyPrefix: String = s"$CONFIG_KEY_PREFIX.opts."

        Map.newBuilder[String, String]
            .tap { builder =>
                systemConfig.getKeys.asScala.foreach { k =>
                    if (k.startsWith(optsKeyPrefix)) {
                        val propKey = k.substring(optsKeyPrefix.length)
                        val propVal = systemConfig.get(k, classOf[String])
                        builder.addOne(propKey -> propVal)
                    }
                }
            }
            .pipe { builder =>
                builder.result()
            }
    }
}

