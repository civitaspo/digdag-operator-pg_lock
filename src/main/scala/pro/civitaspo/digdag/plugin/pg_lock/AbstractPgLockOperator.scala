package pro.civitaspo.digdag.plugin.pg_lock


import io.digdag.client.config.Config
import io.digdag.spi.OperatorContext
import io.digdag.util.BaseOperator


abstract class AbstractPgLockOperator(operatorName: String,
                                      context: OperatorContext,
                                      systemConfig: Config)
    extends BaseOperator(context)
{

}
