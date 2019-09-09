package pro.civitaspo.digdag.plugin.pg_lock


import java.util.{List => JList}

import com.google.common.collect.ImmutableList
import com.google.inject.Inject
import io.digdag.client.config.Config
import io.digdag.spi.{OperatorFactory, OperatorProvider}


class PgLockOperatorProvider
    extends OperatorProvider
{

    @Inject protected var systemConfig: Config = null
    @Inject protected var pooler: PgLockPostgresqlConnectionPooler = null

    override def get(): JList[OperatorFactory] =
    {
        ImmutableList.of(
            new PgLockOperatorFactory(systemConfig, pooler),
            new PgLockReleaseOperatorFactory(systemConfig, pooler)
        )
    }
}
