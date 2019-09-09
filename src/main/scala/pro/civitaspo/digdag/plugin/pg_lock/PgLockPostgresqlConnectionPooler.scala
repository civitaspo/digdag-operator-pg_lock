package pro.civitaspo.digdag.plugin.pg_lock

import java.sql.SQLException

import com.google.common.base.Throwables
import com.typesafe.scalalogging.LazyLogging
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.skife.jdbi.v2.{DBI, Handle}
import org.skife.jdbi.v2.exceptions.TransactionFailedException

import scala.util.chaining._


class PgLockPostgresqlConnectionPooler(systemConfig: PgLockOperatorSystemConfig)
    extends LazyLogging
{
    private val hikari: HikariDataSource = createDataSourceWithConnectionPool().tap { hikari =>
        // TODO: ignore errors?
        if (systemConfig.schemaMigration) PgLockPostgresqlDatabaseMigrator(hikari).migrate()
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
        hc.setJdbcUrl(systemConfig.jdbcUrl)
        hc.setDriverClassName(systemConfig.driverClassName)
        hc.setDataSourceProperties(systemConfig.jdbcProperties)
        hc.setConnectionTimeout(
            systemConfig.connectionTimeout.getDuration.toMillis
            )
        hc.setIdleTimeout(
            systemConfig.idleTimeout.getDuration.toMillis
            )
        hc.setValidationTimeout(
            systemConfig.validationTimeout.getDuration.toMillis
            )
        hc.setLeakDetectionThreshold(
            systemConfig.leakDetectionThreshold.getDuration.toMillis
            )
        hc.setMaxLifetime(
            systemConfig.maxLifeTime.getDuration.toMillis
            )
        hc.setMaximumPoolSize(systemConfig.maxPoolSize)
        hc.setMinimumIdle(systemConfig.minIdleSize)

        // Here should not set connectionTestQuery (that overrides isValid) because
        // ThreadLocalTransactionManager.commit assumes that Connection.isValid returns
        // false when an error happened during a transaction.

        new HikariDataSource(hc)
    }
}
