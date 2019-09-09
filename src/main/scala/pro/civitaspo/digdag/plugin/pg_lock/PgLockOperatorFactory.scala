package pro.civitaspo.digdag.plugin.pg_lock


import io.digdag.client.config.Config
import io.digdag.spi.{Operator, OperatorContext}


class PgLockOperatorFactory(systemConfig: Config,
                            pooler: PgLockPostgresqlConnectionPooler)
    extends AbstractPgLockOperatorFactory(systemConfig, pooler)
{
    override def getType: String =
    {
        "pg_lock"
    }

    override def newOperator(context: OperatorContext): Operator =
    {
        new PgLockOperator(
            operatorName = getType,
            context = context,
            systemConfig = PgLockOperatorSystemConfig(systemConfig = systemConfig),
            pgClient = PgLockPostgresqlClient(handle = pooler.getConnection)
        )
    }
}
