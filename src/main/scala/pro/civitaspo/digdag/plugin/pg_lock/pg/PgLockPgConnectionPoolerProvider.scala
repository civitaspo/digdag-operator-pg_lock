package pro.civitaspo.digdag.plugin.pg_lock.pg


import com.google.inject.{Inject, Provider}
import com.typesafe.scalalogging.LazyLogging
import io.digdag.client.config.Config
import javax.annotation.PreDestroy


class PgLockPgConnectionPoolerProvider
    extends Provider[PgLockPgConnectionPooler]
        with LazyLogging
{
    @Inject protected var systemConfig: Config = null

    lazy private val pooler: PgLockPgConnectionPooler =
        new PgLockPgConnectionPooler(config = PgLockPgConfig(systemConfig))

    override def get(): PgLockPgConnectionPooler =
    {
        pooler
    }


    @PreDestroy
    def shutdown(): Unit =
    {
        logger.info("shutdown called: {}")
        pooler.synchronized {
            pooler.shutdown()
        }
    }
}
