package pro.civitaspo.digdag.plugin.pg_lock


import io.digdag.client.config.Config
import io.digdag.spi.OperatorFactory


abstract class AbstractPgLockOperatorFactory[A <: AbstractPgLockOperator](
    systemConfig: Config,
    pooler: PgLockPostgresqlConnectionPooler)
    extends OperatorFactory
{
}
