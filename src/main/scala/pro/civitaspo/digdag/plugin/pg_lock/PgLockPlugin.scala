package pro.civitaspo.digdag.plugin.pg_lock


import com.google.inject.{Binder, Scopes}
import com.typesafe.scalalogging.LazyLogging
import io.digdag.spi.{OperatorProvider, Plugin}
import pro.civitaspo.digdag.plugin.pg_lock.pg.{PgLockPgConnectionPooler, PgLockPgConnectionPoolerLifecycleManager, PgLockPgConnectionPoolerLifecycleManagerProvider, PgLockPgConnectionPoolerProvider}


class PgLockPlugin
    extends Plugin
        with LazyLogging
{
    override def getServiceProvider[T](`type`: Class[T]): Class[_ <: T] =
    {
        if (`type` ne classOf[OperatorProvider]) return null
        classOf[PgLockOperatorProvider].asSubclass(`type`)
    }

    override def configureBinder[T](`type`: Class[T],
                                    binder: Binder): Unit =
    {
        super.configureBinder(`type`, binder)
        binder
            .bind(classOf[PgLockPgConnectionPooler])
            .toProvider(classOf[PgLockPgConnectionPoolerProvider])
            .in(Scopes.SINGLETON)
        binder
            .bind(classOf[PgLockPgConnectionPoolerLifecycleManager])
            .toProvider(classOf[PgLockPgConnectionPoolerLifecycleManagerProvider])
            .in(Scopes.SINGLETON)
    }
}
