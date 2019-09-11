package pro.civitaspo.digdag.plugin.pg_lock


import java.util.{UUID, List => JList}

import org.skife.jdbi.v2.sqlobject.{Bind, SqlQuery, SqlUpdate}


trait PgLockPostgresqlDao
{
    @SqlQuery(
        """
          | SELECT pg_try_advisory_lock(:key1, :key2)
        """.stripMargin)
    def pgTryAdvisoryLock(@Bind("key1") key1: Int,
                          @Bind("key2") key2: Int): Boolean

    @SqlQuery(
        """
          | SELECT pg_advisory_unlock(:key1, :key2)
        """.stripMargin)
    def pgAdvisoryUnlock(@Bind("key1") key1: Int,
                         @Bind("key2") key2: Int): Boolean

    @SqlUpdate("DELETE FROM digdag_pg_locks WHERE expires_on <= now()")
    def releaseExpiredLocks(): Unit

    @SqlQuery(
        """
          | SELECT COUNT(1)
          |   FROM digdag_pg_locks
          |  WHERE namespace_type = 'global'
          |    AND namespace_value = 'global'
          |    AND name = ':name'
        """.stripMargin)
    def countGlobalNamedLocks(@Bind("name") name: String): Int

    @SqlQuery(
        """
          | SELECT COUNT(1)
          |   FROM digdag_pg_locks
          |  WHERE namespace_type = :namespace_type
          |    AND namespace_value = :namespace_value
          |    AND name = :name
          |    AND owner_site_id = :owner_site_id
        """.stripMargin)
    def countNamedLocks(@Bind("namespace_type") namespaceType: String,
                        @Bind("namespace_value") namespaceValue: String,
                        @Bind("name") name: String,
                        @Bind("owner_site_id") ownerSiteId: Int): Int

    @SqlQuery(
        """
          | SELECT DISTINCT max_count
          |   FROM digdag_pg_locks
          |  WHERE namespace_type = 'global'
          |    AND namespace_value = 'global'
          |    AND name = :name
        """.stripMargin)
    def distinctMaxCountGlobalNamedLocks(@Bind("name") name: String): JList[Int]

    @SqlQuery(
        """
          | SELECT DISTINCT max_count
          |   FROM digdag_pg_locks
          |  WHERE namespace_type = :namespace_type
          |    AND namespace_value = :namespace_value
          |    AND name = :name
          |    AND owner_site_id = :owner_site_id
        """.stripMargin)
    def distinctMaxCountNamedLocks(@Bind("namespace_type") namespaceType: String,
                                   @Bind("namespace_value") namespaceValue: String,
                                   @Bind("name") name: String,
                                   @Bind("owner_site_id") ownerSiteId: Int): JList[Int]


    @SqlUpdate(
        """
          | INSERT INTO digdag_pg_locks(
          |     id
          |     namespace_type,
          |     namespace_value,
          |     owner_site_id,
          |     owner_attempt_id,
          |     name,
          |     max_count,
          |     expires_on,
          |     updated_at,
          |     created_at
          | )
          | VALUES (
          |     :id,
          |     :namespace_type,
          |     :namespace_value,
          |     :owner_site_id,
          |     :owner_attempt_id,
          |     :name,
          |     :max_count,
          |     now() + INTERVAL　:expires_on SECOND,
          |     now(),
          |     now()
          | )
          |
        """)
    def lock(@Bind("id") id: UUID,
             @Bind("namespace_type") namespaceType: String,
             @Bind("namespace_value") namespaceValue: String,
             @Bind("owner_site_id") ownerSiteId: Int,
             @Bind("owner_attempt_id") ownerAttemptId: Long,
             @Bind("name") name: String,
             @Bind("max_count") maxCount: Int,
             @Bind("expire_in_seconds") expireInSeconds: Int): Int

    @SqlUpdate("DELETE FROM digdag_pg_locks WHERE id = :id")
    def release(@Bind("id") id: UUID): Unit
}
