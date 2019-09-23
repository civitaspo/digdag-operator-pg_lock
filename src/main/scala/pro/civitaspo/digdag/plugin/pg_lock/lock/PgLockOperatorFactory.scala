package pro.civitaspo.digdag.plugin.pg_lock.lock


import io.digdag.client.config.Config
import io.digdag.spi.{Operator, OperatorContext, OperatorFactory}
import pro.civitaspo.digdag.plugin.pg_lock.pg.{PgLockPgClient, PgLockPgConnectionPooler}


case class PgLockOperatorFactory(systemConfig: Config,
                                 pooler: PgLockPgConnectionPooler)
    extends OperatorFactory
{
    override def getType: String =
    {
        "pg_lock"
    }

    override def newOperator(context: OperatorContext): Operator =
    {
        val pgClient = PgLockPgClient(handle = pooler.getConnection)
        new PgLockOperator(context = context, pgClient = pgClient, systemConfig = systemConfig)
    }
}
