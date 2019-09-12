package pro.civitaspo.digdag.plugin.pg_lock


import java.nio.charset.StandardCharsets
import java.util.UUID

import io.digdag.client.config.{Config, ConfigException}
import io.digdag.spi.{OperatorContext, TaskResult}
import io.digdag.util.DurationParam


class PgLockOperator(operatorName: String,
                     context: OperatorContext,
                     systemConfig: PgLockOperatorSystemConfig,
                     pgClient: PgLockPostgresqlClient)
    extends AbstractPgLockOperator(operatorName, context, systemConfig, pgClient)
{
    sealed abstract class LockNamespace(nsType: String,
                                        nsValue: UUID,
                                        ownerAttemptId: Long = request.getAttemptId)
    object LockNamespace
    {
        private val SEPARATOR: String = "\u001F"

        private def toUUID(str: String): UUID =
        {
            UUID.nameUUIDFromBytes(str.getBytes(StandardCharsets.UTF_8))
        }

        private def toUUIDWithSiteId(str: String): UUID =
        {
            toUUID(Seq(request.getSiteId.toString, str).mkString(SEPARATOR))
        }

        case object Global
            extends LockNamespace(nsType = "global",
                                  nsValue = toUUID("global"))
        case object Site
            extends LockNamespace(nsType = "site",
                                  nsValue = toUUID(request.getSiteId.toString))
        case object Project
            extends LockNamespace(nsType = "project",
                                  nsValue = toUUIDWithSiteId(request.getProjectId.toString))
        case object Workflow
            extends LockNamespace(nsType = "workflow",
                                  nsValue = toUUIDWithSiteId(request.getWorkflowName))
        case object Session
            extends LockNamespace(nsType = "session",
                                  nsValue = toUUIDWithSiteId(request.getSessionUuid.toString))
        case object Attempt
            extends LockNamespace(nsType = "attempt",
                                  nsValue = toUUIDWithSiteId(request.getAttemptId.toString))
        def apply(name: String): LockNamespace =
        {
            name match {
                case "global"    => Global
                case "site"      => Site
                case "project"   => Project
                case "workflow"  => Session
                case "attempt"   => Attempt
                case unsupported => throw new ConfigException(s"Unsupported namespace: $unsupported")
            }
        }
    }

    case class PgLockOperatorConfig(
        lockName: String,
        waitTimeout: DurationParam,
        expire_in: DurationParam,
        max_count: Int,
        namespace: LockNamespace,
        doConfig: Config
    )

    override def runTask(): TaskResult =
    {
        val operatorConfig: PgLockOperatorConfig = PgLockOperatorConfig(
            lockName = params.get("_command", classOf[String]),
            waitTimeout = params.get("wait_timeout",
                                     classOf[DurationParam],
                                     DurationParam.parse("15m")
                                     ),
            expire_in = params.get("expire_in",
                                   classOf[DurationParam],
                                   DurationParam.parse("1h")
                                   ),
            max_count = params.get("max_count", classOf[Int], 1),
            namespace = LockNamespace(params.get("namespace",
                                                 classOf[String],
                                                 "site"
                                                 )),
            doConfig = params.get("_do", classOf[Config])
            )

        runTask(operatorConfig)
    }

    protected def runTask(operatorConfig: PgLockOperatorConfig): TaskResult =
    {
        TaskResult.empty(cf)
    }
}
