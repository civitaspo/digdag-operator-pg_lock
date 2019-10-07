package pro.civitaspo.digdag.plugin.pg_lock.pg


import com.google.inject.{Inject, Provider}
import io.digdag.client.config.Config
import javax.annotation.PreDestroy


class PgLockPgConnectionPoolerProvider
    extends Provider[PgLockPgConnectionPooler]
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
        pooler.synchronized {
            pooler.shutdown()
        }
    }
}
