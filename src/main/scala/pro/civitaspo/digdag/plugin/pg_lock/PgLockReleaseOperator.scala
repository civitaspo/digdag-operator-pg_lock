package pro.civitaspo.digdag.plugin.pg_lock


import io.digdag.spi.{OperatorContext, TaskResult}


class PgLockReleaseOperator(operatorName: String,
                            context: OperatorContext,
                            systemConfig: PgLockOperatorSystemConfig,
                            pgClient: PgLockPostgresqlClient)
    extends AbstractPgLockOperator(operatorName, context, systemConfig, pgClient)
{
    override def runTask(): TaskResult =
    {
        TaskResult.empty(getConfigFactory)
    }
}
