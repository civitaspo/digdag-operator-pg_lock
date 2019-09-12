package pro.civitaspo.digdag.plugin.pg_lock


import com.typesafe.scalalogging.LazyLogging
import io.digdag.client.config.{Config, ConfigFactory}
import io.digdag.spi.OperatorContext
import io.digdag.util.BaseOperator


abstract class AbstractPgLockOperator(operatorName: String,
                                      context: OperatorContext,
                                      systemConfig: PgLockOperatorSystemConfig,
                                      pgClient: PgLockPostgresqlClient)
    extends BaseOperator(context)
        with LazyLogging
{
    protected def getConfigFactory: ConfigFactory =
    {
        request.getConfig.getFactory
    }

    protected def getParams: Config =
    {
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
