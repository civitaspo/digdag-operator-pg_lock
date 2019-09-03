package pro.civitaspo.digdag.plugin.pg_lock


import java.util.{Arrays => JArrays, List => JList}

import com.google.inject.Inject
import io.digdag.client.config.Config
import io.digdag.spi.{OperatorFactory, OperatorProvider}


class PgLockOperatorProvider
    extends OperatorProvider
{

    @Inject protected var systemConfig: Config = null

    override def get(): JList[OperatorFactory] =
    {
        val off = PgLockOperatorFactoryFactory(systemConfig = systemConfig)

        JArrays.asList(
            off.createFactory("pg_lock", classOf[PgLockOperator]),
            off.createFactory("pg_lock_release", classOf[PgLockReleaseOperator])
            )
    }
}
