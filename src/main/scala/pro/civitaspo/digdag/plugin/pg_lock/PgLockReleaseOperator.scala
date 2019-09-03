package pro.civitaspo.digdag.plugin.pg_lock


import io.digdag.client.config.Config
import io.digdag.spi.{OperatorContext, TaskResult}


class PgLockReleaseOperator(operatorName: String,
                            context: OperatorContext,
                            systemConfig: Config)
    extends AbstractPgLockOperator(operatorName, context, systemConfig)
{
    override def runTask(): TaskResult =
    {
        null
    }
}
