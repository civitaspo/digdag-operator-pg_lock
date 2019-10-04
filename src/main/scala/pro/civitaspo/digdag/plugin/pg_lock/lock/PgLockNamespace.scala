package pro.civitaspo.digdag.plugin.pg_lock.lock


import java.nio.charset.StandardCharsets
import java.util.UUID

import io.digdag.client.config.ConfigException
import io.digdag.spi.TaskRequest


sealed abstract class PgLockNamespace(`type`: String,
                                      value: UUID,
                                      ownerAttemptId: Long)
{
    def getType: String =
    {
        `type`
    }

    def getValue: UUID =
    {
        value
    }

    def getOwnerAttemptId: Long =
    {
        ownerAttemptId
    }
}

object PgLockNamespace
{
    case class Factory(request: TaskRequest)
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
            extends PgLockNamespace(`type` = "global",
                                    value = toUUID("global"),
                                    ownerAttemptId = request.getAttemptId)
        case object Site
            extends PgLockNamespace(`type` = "site",
                                    value = toUUID(request.getSiteId.toString),
                                    ownerAttemptId = request.getAttemptId)
        case object Project
            extends PgLockNamespace(`type` = "project",
                                    value = toUUIDWithSiteId(request.getProjectId.toString),
                                    ownerAttemptId = request.getAttemptId)
        case object Workflow
            extends PgLockNamespace(`type` = "workflow",
                                    value = toUUIDWithSiteId(request.getWorkflowName),
                                    ownerAttemptId = request.getAttemptId)
        case object Session
            extends PgLockNamespace(`type` = "session",
                                    value = toUUIDWithSiteId(request.getSessionUuid.toString),
                                    ownerAttemptId = request.getAttemptId)
        case object Attempt
            extends PgLockNamespace(`type` = "attempt",
                                    value = toUUIDWithSiteId(request.getAttemptId.toString),
                                    ownerAttemptId = request.getAttemptId)
        def create(name: String): PgLockNamespace =
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
}
