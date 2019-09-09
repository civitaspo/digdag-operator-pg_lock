package pro.civitaspo.digdag.plugin.pg_lock


import com.google.inject.{Binder, Module, Scopes}


class PgLockPluginModule
    extends Module
{
    override def configure(binder: Binder): Unit =
    {
        binder
            .bind(classOf[PgLockPostgresqlConnectionPooler])
            .toProvider(classOf[PgLockPostgresqlConnectionPoolerProvider])
            .in(Scopes.SINGLETON)
    }
}
