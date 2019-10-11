package pro.civitaspo.digdag.plugin.pg_lock.pg


import com.google.inject.{Inject, Provider}
import javax.annotation.PreDestroy


class PgLockPgConnectionPoolerLifecycleManagerProvider
    extends Provider[PgLockPgConnectionPoolerLifecycleManager]
{
    @Inject protected var pooler: PgLockPgConnectionPooler = null

    private lazy val lifecycleManager: PgLockPgConnectionPoolerLifecycleManager =
        new PgLockPgConnectionPoolerLifecycleManager(pooler)

    override def get(): PgLockPgConnectionPoolerLifecycleManager =
    {
        lifecycleManager
    }


    @PreDestroy
    def shutdown(): Unit =
    {
        lifecycleManager.onShutdown()
    }
}
