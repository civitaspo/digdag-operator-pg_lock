package pro.civitaspo.digdag.plugin.pg_lock.pg


import org.skife.jdbi.v2.Handle


case class PgLockPgClient(handle: Handle)
    extends AutoCloseable
{
    private var doneTransactionOnce: Boolean = false

    def transaction[A](f: PgLockPgDao => A): A =
    {
        if (doneTransactionOnce) throw new IllegalStateException("PgLockPostgresqlClient cannot execute transaction twice.")
        doneTransactionOnce = true
        try {
            f(handle.attach(classOf[PgLockPgDao]))
        }
        catch {
            case e: Exception =>
                rollback()
                throw e
        }
        finally {
            close()
        }

    }

    def rollback(): Unit =
    {
        // When the handle is created, handle.begin() is always called
        // at PgLockPostgresqlConnectionPooler#getConnection().
        // So we must call commit() or rollback() to close the transaction.
        handle.rollback()
    }

    def commit(): Unit =
    {
        // When the handle is created, handle.begin() is always called
        // at PgLockPostgresqlConnectionPooler#getConnection().
        // So we must call commit() or rollback() to close the transaction.
        handle.commit()
    }

    override def close(): Unit =
    {
        // `handle` object is created for every time when ParamServerClientConnection
        // is injected, and is not a singleton object.
        // So we must close connection manually to return the connection to the connection pool.
        handle.close()
    }
}
