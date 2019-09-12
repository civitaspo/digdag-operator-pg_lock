package pro.civitaspo.digdag.plugin.pg_lock


import java.nio.charset.StandardCharsets
import java.util.UUID

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
}
