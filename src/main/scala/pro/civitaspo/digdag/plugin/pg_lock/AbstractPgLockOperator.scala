package pro.civitaspo.digdag.plugin.pg_lock


import com.typesafe.scalalogging.LazyLogging
import io.digdag.client.config.{Config, ConfigException, ConfigFactory}
import io.digdag.spi.OperatorContext
import io.digdag.util.BaseOperator


abstract class AbstractPgLockOperator(operatorName: String,
                                      context: OperatorContext,
                                      systemConfig: PgLockOperatorSystemConfig,
                                      pgClient: PgLockPostgresqlClient)
    extends BaseOperator(context)
        with LazyLogging
{
    protected val cf: ConfigFactory = request.getConfig.getFactory
    protected val params: Config = {
        val elems: Seq[String] = operatorName.split("\\.").toSeq
        elems.indices.foldLeft(request.getConfig) { (p: Config,
                                                     idx: Int) =>
            p.mergeDefault((0 to idx).foldLeft(request.getConfig) { (nestedParam: Config,
                                                                     keyIdx: Int) =>
                nestedParam.getNestedOrGetEmpty(elems(keyIdx))
            })
        }
    }

    protected val hasher: PgLockHasher = PgLockHasher(systemConfig.hashSeedForAdvisoryLock)

    sealed abstract class LockNamespace(`type`: String)
    {
        def getType: String =
        {
            `type`
        }

        def getValue: String

        def getOwnerSiteId: Int =
        {
            request.getSiteId
        }

        def getOwnerAttemptId: Long =
        {
            request.getAttemptId
        }
    }
    object LockNamespace
    {
        case object Global
            extends LockNamespace("global")
        {
            override def getValue: String =
            {
                "global"
            }
        }
        case object Site
            extends LockNamespace("site")
        {
            override def getValue: String =
            {
                request.getSiteId.toString
            }
        }
        case object Project
            extends LockNamespace("project")
        {
            override def getValue: String =
            {
                request.getProjectId.toString
            }
        }
        case object Workflow
            extends LockNamespace("workflow")
        {
            override def getValue: String =
            {
                request.getWorkflowName
            }
        }
        case object Session
            extends LockNamespace("session")
        {
            override def getValue: String =
            {
                request.getSessionUuid.toString
            }
        }
        case object Attempt
            extends LockNamespace("attempt")
        {
            override def getValue: String =
            {
                request.getAttemptId.toString
            }
        }

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
}
