package pro.civitaspo.digdag.plugin.pg_lock

import com.google.inject.{Inject, Provider}
import io.digdag.client.config.Config

class PgLockPostgresqlConnectionPoolerProvider
    extends Provider[PgLockPostgresqlConnectionPooler]
{
    @Inject protected var systemConfig: Config = null

    override def get(): PgLockPostgresqlConnectionPooler =
    {
        new PgLockPostgresqlConnectionPooler(systemConfig = PgLockOperatorSystemConfig(systemConfig))
    }
}
