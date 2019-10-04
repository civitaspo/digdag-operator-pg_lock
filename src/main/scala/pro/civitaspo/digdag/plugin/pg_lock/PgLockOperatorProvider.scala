package pro.civitaspo.digdag.plugin.pg_lock


import java.util.{List => JList}

import com.google.common.collect.ImmutableList
import com.google.inject.Inject
import io.digdag.client.config.Config
import io.digdag.spi.{OperatorFactory, OperatorProvider}
import pro.civitaspo.digdag.plugin.pg_lock.lock.PgLockOperatorFactory
import pro.civitaspo.digdag.plugin.pg_lock.pg.PgLockPgConnectionPooler
import pro.civitaspo.digdag.plugin.pg_lock.unlock.PgUnlockOperatorFactory


class PgLockOperatorProvider
    extends OperatorProvider
{

    @Inject protected var systemConfig: Config = null
    @Inject protected var pooler: PgLockPgConnectionPooler = null

    override def get(): JList[OperatorFactory] =
    {
        ImmutableList.of(
            PgLockOperatorFactory(systemConfig, pooler),
            PgUnlockOperatorFactory(systemConfig, pooler)
            )
    }
}
