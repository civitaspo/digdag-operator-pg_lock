package pro.civitaspo.digdag.plugin.pg_lock.pg


import java.sql.SQLException

import com.google.common.base.Throwables
import com.typesafe.scalalogging.LazyLogging
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.skife.jdbi.v2.{DBI, Handle}
import org.skife.jdbi.v2.exceptions.TransactionFailedException
import org.skife.jdbi.v2.logging.SLF4JLog
import org.skife.jdbi.v2.logging.SLF4JLog.Level

import scala.util.chaining._


class PgLockPgConnectionPooler(config: PgLockPgConfig)
    extends LazyLogging
{
    private val hikari: HikariDataSource = createDataSourceWithConnectionPool().tap { hikari =>
        logger.info("hikari: {} is initialized", hikari)
        // TODO: ignore errors?
        if (config.schemaMigration) PgLockPgDatabaseMigrator(config, hikari).migrate()
    }
    private val dbi: DBI = new DBI(hikari)

    def getConnection: Handle =
    {
        dbi.open().tap { handle =>
            try handle.getConnection.setAutoCommit(false)
            catch {
                case ex: SQLException =>
                    throw new TransactionFailedException("Failed to set auto commit: " + false, ex)
            }
            handle.setSQLLog(new SLF4JLog(logger.underlying, Level.DEBUG))
            handle.begin()
        }
    }

    def shutdown(): Unit =
    {
        try hikari.close()
        catch {
            case ex: Exception =>
                throw Throwables.propagate(ex)
        }
    }

    private def createDataSourceWithConnectionPool(): HikariDataSource =
    {
        val hc: HikariConfig = new HikariConfig()
        hc.setJdbcUrl(config.jdbcUrl)
        hc.setDriverClassName(config.driverClassName)
        hc.setDataSourceProperties(config.jdbcProperties)
        hc.setConnectionTimeout(
            config.connectionTimeout.getDuration.toMillis
            )
        hc.setIdleTimeout(
            config.idleTimeout.getDuration.toMillis
            )
        hc.setValidationTimeout(
            config.validationTimeout.getDuration.toMillis
            )
        hc.setLeakDetectionThreshold(
            config.leakDetectionThreshold.getDuration.toMillis
            )
        hc.setMaxLifetime(
            config.maxLifeTime.getDuration.toMillis
            )
        hc.setMaximumPoolSize(config.maxPoolSize)
        hc.setMinimumIdle(config.minIdleSize)

        // Here should not set connectionTestQuery (that overrides isValid) because
        // ThreadLocalTransactionManager.commit assumes that Connection.isValid returns
        // false when an error happened during a transaction.

        new HikariDataSource(hc)
    }

    /*
      TODO: This class depends on the `finalize()` method to close the pool, it is not a good way.
            But, Guice Injector does not manage the lifecycle, so I have to take the way...
            ref. https://github.com/google/guice/issues/1069
            If someone else has a better way, please tell me or give me a pull-request.
    */

    override def finalize(): Unit =
    {
        shutdown()
    }
}
