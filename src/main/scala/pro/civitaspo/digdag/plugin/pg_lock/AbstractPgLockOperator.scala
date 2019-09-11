package pro.civitaspo.digdag.plugin.pg_lock


import com.typesafe.scalalogging.LazyLogging
import io.digdag.client.config.{Config, ConfigFactory}
import io.digdag.spi.{OperatorContext, SecretProvider}
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
    protected val secrets: SecretProvider = context.getSecrets.getSecrets("pg_lock")
    protected val sessionUuid: String = params.get("session_uuid", classOf[String])


    protected val hasher: PgLockHasher = PgLockHasher(systemConfig.hashSeedForAdvisoryLock)
}
