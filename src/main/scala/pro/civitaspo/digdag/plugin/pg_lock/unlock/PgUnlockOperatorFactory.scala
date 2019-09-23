package pro.civitaspo.digdag.plugin.pg_lock.unlock


import io.digdag.client.config.Config
import io.digdag.spi.{Operator, OperatorContext, OperatorFactory}
import pro.civitaspo.digdag.plugin.pg_lock.pg.{PgLockPgClient, PgLockPgConnectionPooler}


case class PgUnlockOperatorFactory(systemConfig: Config,
                                   pooler: PgLockPgConnectionPooler)
    extends OperatorFactory
{
    override def getType: String =
    {
        "pg_unlock"
    }

    override def newOperator(context: OperatorContext): Operator =
    {
        val pgClient = PgLockPgClient(handle = pooler.getConnection)
        new PgUnlockOperator(context = context, systemConfig = systemConfig, pgClient = pgClient)
    }
}
