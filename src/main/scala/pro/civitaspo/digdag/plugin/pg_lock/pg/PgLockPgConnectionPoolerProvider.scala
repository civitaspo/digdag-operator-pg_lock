package pro.civitaspo.digdag.plugin.pg_lock.pg


import com.google.inject.{Inject, Provider}
import com.typesafe.scalalogging.LazyLogging
import io.digdag.client.config.Config
import javax.annotation.PreDestroy

import scala.util.chaining._


class PgLockPgConnectionPoolerProvider
    extends Provider[PgLockPgConnectionPooler]
        with LazyLogging
{
    @Inject protected var systemConfig: Config = null

    lazy private val pooler: PgLockPgConnectionPooler =
        PgLockPgConnectionPoolerProvider.getOrCreate(systemConfig)

    override def get(): PgLockPgConnectionPooler =
    {
        pooler
    }


    @PreDestroy
    def shutdown(): Unit =
    {
        logger.info("shutdown called: {}")
        PgLockPgConnectionPoolerProvider.shutdown()
    }
}

object PgLockPgConnectionPoolerProvider
{
    var pooler: Option[PgLockPgConnectionPooler] = None

    def getOrCreate(systemConfig: Config): PgLockPgConnectionPooler =
    {
        pooler.synchronized {
            pooler.getOrElse {
                new PgLockPgConnectionPooler(config = PgLockPgConfig(systemConfig)).tap { p => pooler = Option(p) }
            }
        }
    }

    def shutdown(): Unit =
    {
        pooler.synchronized {
            if (pooler.isDefined) pooler.get.shutdown()
            pooler = None
        }
    }
}