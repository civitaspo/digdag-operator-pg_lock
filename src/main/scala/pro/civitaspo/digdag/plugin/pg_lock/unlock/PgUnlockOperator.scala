package pro.civitaspo.digdag.plugin.pg_lock.unlock


import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import io.digdag.client.config.Config
import io.digdag.spi.{OperatorContext, TaskResult}
import io.digdag.util.BaseOperator
import pro.civitaspo.digdag.plugin.pg_lock.pg.PgLockPgClient


class PgUnlockOperator(context: OperatorContext,
                       systemConfig: Config,
                       pgClient: PgLockPgClient)
    extends BaseOperator(context)
        with LazyLogging
{
    override def runTask(): TaskResult =
    {
        try OperatorRunner().run()
        finally pgClient.close()
    }

    case class OperatorRunner()
    {
        val id: UUID = request.getConfig
            .get("_command", classOf[UUID])

        def run(): TaskResult =
        {
            pgClient.transaction { dao =>
                logger.info(s"Release lock: $id")
                dao.deleteDigdagPgLock(id = id)
                pgClient.commit()
                TaskResult.empty(request)
            }
        }
    }
}
