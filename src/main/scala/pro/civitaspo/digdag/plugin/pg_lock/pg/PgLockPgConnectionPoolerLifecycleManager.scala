package pro.civitaspo.digdag.plugin.pg_lock.pg


/*
TODO: This class manages the lifecycle of PgLockPgConnectionPooler.
      This class depends on the `finalize()` method, it is not a good way.
      But, Guice Injector does not manage the lifecycle, so I have to take the way...
      ref. https://github.com/google/guice/issues/1069
      If someone else has a better way, please tell me or give me a pull-request.
 */
class PgLockPgConnectionPoolerLifecycleManager(pooler: PgLockPgConnectionPooler)
{
    override def finalize(): Unit =
    {
        super.finalize()
        onShutdown()
    }

    def onShutdown(): Unit =
    {
        pooler.shutdown()
    }
}
