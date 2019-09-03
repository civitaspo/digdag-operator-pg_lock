package pro.civitaspo.digdag.plugin.pg_lock


import io.digdag.spi.{OperatorProvider, Plugin}


class PgLockPlugin
    extends Plugin
{
    override def getServiceProvider[T](`type`: Class[T]): Class[_ <: T] =
    {
        if (`type` ne classOf[OperatorProvider]) return null
        classOf[PgLockOperatorProvider].asSubclass(`type`)
    }
}
