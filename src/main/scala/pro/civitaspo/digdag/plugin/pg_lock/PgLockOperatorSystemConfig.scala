package pro.civitaspo.digdag.plugin.pg_lock


import java.util.{Properties => JProperties}

import io.digdag.client.config.Config
import io.digdag.util.DurationParam

import scala.jdk.CollectionConverters._
import scala.util.chaining._


case class PgLockOperatorSystemConfig(systemConfig: Config)
{
    val CONFIG_KEY_PREFIX: String = "pg_lock"

    def host: String =
    {
        systemConfig.get(s"$CONFIG_KEY_PREFIX.host", classOf[String])
    }

    def port: Int =
    {
        systemConfig.get(s"$CONFIG_KEY_PREFIX.port", classOf[Int], 5432)
    }

    def database: String =
    {
        systemConfig.get(s"$CONFIG_KEY_PREFIX.database", classOf[String])
    }

    def user: String =
    {
        systemConfig.get(s"$CONFIG_KEY_PREFIX.user", classOf[String])
    }

    def password: Option[String] =
    {
        Option(systemConfig.getOptional(s"$CONFIG_KEY_PREFIX.password",
                                        classOf[String]
                                        ).orNull())
    }

    def loginTimeout: DurationParam =
    {
        systemConfig.get(s"$CONFIG_KEY_PREFIX.login_timeout",
                         classOf[DurationParam],
                         DurationParam.parse("30s")
                         )
    }

    def socketTimeout: DurationParam =
    {
        systemConfig.get(s"$CONFIG_KEY_PREFIX.socket_timeout",
                         classOf[DurationParam],
                         DurationParam.parse("30m")
                         )
    }

    def ssl: Boolean =
    {
        systemConfig.get(s"$CONFIG_KEY_PREFIX.ssl", classOf[Boolean], false)
    }

    def connectionTimeout: DurationParam =
    {
        systemConfig.get(s"$CONFIG_KEY_PREFIX.connection_timeout",
                         classOf[DurationParam],
                         DurationParam.parse("30s")
                         )
    }

    def idleTimeout: DurationParam =
    {
        systemConfig.get(s"$CONFIG_KEY_PREFIX.idle_timeout",
                         classOf[DurationParam],
                         DurationParam.parse("10m")
                         )
    }

    def validationTimeout: DurationParam =
    {
        systemConfig.get(s"$CONFIG_KEY_PREFIX.validation_timeout",
                         classOf[DurationParam],
                         DurationParam.parse("5s")
                         )
    }

    def maxPoolSize: Int =
    {
        systemConfig.get(s"$CONFIG_KEY_PREFIX.max_pool_size", classOf[Int], 5)
    }

    def minIdleSize: Int =
    {
        systemConfig.get(s"$CONFIG_KEY_PREFIX.min_idle_size", classOf[Int], maxPoolSize)
    }

    def maxLifeTime: DurationParam =
    {
        systemConfig.get(s"$CONFIG_KEY_PREFIX.max_life_time",
                         classOf[DurationParam],
                         DurationParam.parse("30m")
                         )
    }

    def leakDetectionThreshold: DurationParam =
    {
        systemConfig.get(s"$CONFIG_KEY_PREFIX.leak_detection_threshold",
                         classOf[DurationParam],
                         DurationParam.parse("0s")
                         )
    }

    def pollInterval: DurationParam =
    {
        systemConfig.get(s"$CONFIG_KEY_PREFIX.poll_interval",
                         classOf[DurationParam],
                         DurationParam.parse("5s")
                         )
    }

    def minPollInterval: DurationParam =
    {
        systemConfig.get(s"$CONFIG_KEY_PREFIX.min_poll_interval",
                         classOf[DurationParam],
                         DurationParam.parse("5s")
                         )
    }

    def maxPollInterval: DurationParam =
    {
        systemConfig.get(s"$CONFIG_KEY_PREFIX.max_poll_interval",
                         classOf[DurationParam],
                         DurationParam.parse("5m")
                         )
    }

    def schemaMigration: Boolean =
    {
        systemConfig.get(s"$CONFIG_KEY_PREFIX.schema_migration", classOf[Boolean], true)
    }

    def hashSeedForAdvisoryLock: Int =
    {
        systemConfig.get(s"$CONFIG_KEY_PREFIX.hash_seed_for_advisory_lock", classOf[Int], -137723950)
    }

    def opts: Map[String, String] =
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

    // combination properties

    def jdbcUrl: String =
    {
        f"jdbc:postgresql://$host:$port/$database"
    }

    def jdbcProperties: JProperties =
    {
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

    def driverClassName: String =
    {
        "org.postgresql.Driver"
    }

}
