package pro.civitaspo.digdag.plugin.pg_lock.pg


import com.google.inject.{Inject, Provider}
import io.digdag.client.config.Config


class PgLockPgConnectionPoolerProvider
    extends Provider[PgLockPgConnectionPooler]
{
    @Inject protected var systemConfig: Config = null

    override def get(): PgLockPgConnectionPooler =
    {
        new PgLockPgConnectionPooler(config = PgLockPgConfig(systemConfig))
    }
}
