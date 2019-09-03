package pro.civitaspo.digdag.plugin.pg_lock


import java.lang.reflect.Constructor

import io.digdag.client.config.{Config, ConfigException}
import io.digdag.spi.{OperatorContext, OperatorFactory}


case class PgLockOperatorFactoryFactory(systemConfig: Config)
{
    def createFactory[T <: AbstractPgLockOperator](operatorName: String,
                                                   operatorClass: Class[T]): OperatorFactory =
    {
        new OperatorFactory
        {
            override def getType: String =
            {
                operatorName
            }

            override def newOperator(context: OperatorContext): T =
            {
                val constructor: Constructor[T] = operatorClass.getConstructor(classOf[String],
                                                                               classOf[OperatorContext],
                                                                               classOf[Config])
                try {
                    constructor.newInstance(operatorName, context, systemConfig)
                }
                catch {
                    case e: Throwable => throw new ConfigException(e)
                }
            }
        }
    }
}
