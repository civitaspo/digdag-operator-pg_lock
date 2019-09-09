package pro.civitaspo.digdag.plugin.pg_lock


import java.util.{UUID, List => JList}

import org.skife.jdbi.v2.sqlobject.{Bind, SqlQuery, SqlUpdate}


trait PgLockPostgresqlDao
{
    @SqlUpdate("DELETE FROM digdag_pg_locks WHERE expires_on <= now()")
    def releaseExpiredLocks(): Unit

    @SqlQuery(
        """
          | SELECT COUNT(1) FROM digdag_pg_locks
          |  WHERE namespace = :namespace
          |    AND namespace_value = :namespace_value
          |    AND name = :name
          |    FOR UPDATE
        """)
    def countLocksInNamespace(@Bind("namespace") namespace: String,
                              @Bind("namespace_value") namespaceValue: String,
                              @Bind("name") name: String): Int

    @SqlQuery(
        """
          | SELECT distinct max_count FROM digdag_pg_locks
          |  WHERE namespace = :namespace
          |    AND namespace_value = :namespace_value
          |    AND name = :name
          |    FOR UPDATE
        """)
    def varietyMaxCountsForLocksInNamespace(@Bind("namespace") namespace: String,
                                            @Bind("namespace_value") namespaceValue: String,
                                            @Bind("name") name: String): JList[Int]


    @SqlUpdate(
        """
          | INSERT INTO digdag_pg_locks(id
          |     namespace,
          |     namespace_value,
          |     owner_attempt_id,
          |     name,
          |     max_count,
          |     expires_on,
          |     updated_at,
          |     created_at
          | )
          | VALUES (
          |     :id,
          |     :namespace,
          |     :namespace_value,
          |     :owner_attempt_id,
          |     :name,
          |     :max_count,
          |     now() + INTERVAL　':expires_on' SECOND,
          |     now(),
          |     now()
          | )
          |
        """)
    def lock(@Bind("id") id: UUID,
             @Bind("namespace") namespace: String,
             @Bind("namespace_value") namespaceValue: String,
             @Bind("owner_attempt_id") ownerAttemptId: String,
             @Bind("name") name: String,
             @Bind("max_count") maxCount: Int,
             @Bind("expire_in_seconds") expireInSeconds: Int): Int

    def generateRandomUUID(): UUID =
    {
        UUID.randomUUID()
    }

    @SqlUpdate("DELETE FROM digdag_pg_locks WHERE id = :id")
    def release(@Bind("id") id: UUID): Unit
}
