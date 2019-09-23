package pro.civitaspo.digdag.plugin.pg_lock.pg


import java.util.UUID

import org.skife.jdbi.v2.sqlobject.{Bind, SqlQuery, SqlUpdate}


trait PgLockPgDao
{
    @SqlQuery("SELECT pg_try_advisory_lock(:key1, :key2)")
    def pgTryAdvisoryLock(@Bind("key1") key1: Int,
                          @Bind("key2") key2: Int): Boolean

    @SqlQuery("SELECT pg_advisory_unlock(:key1, :key2)")
    def pgAdvisoryUnlock(@Bind("key1") key1: Int,
                         @Bind("key2") key2: Int): Boolean

    @SqlUpdate(
        "DELETE" +
            " FROM digdag_pg_locks" +
            " WHERE namespace_type = :namespace_type" +
            " AND namespace_value = :namespace_value" +
            " AND name = :name" +
            " AND expires_on < now()")
    def deleteExpiredDigdagPgLocks(@Bind("namespace_type") namespaceType: String,
                                   @Bind("namespace_value") namespaceValue: UUID,
                                   @Bind("name") name: String): Unit


    @SqlQuery(
        "SELECT COUNT(1) AS cnt" +
            " FROM digdag_pg_locks" +
            " WHERE namespace_type = :namespace_type" +
            " AND namespace_value = :namespace_value" +
            " AND name = :name" +
            " AND expires_on >= now()"
        )
    def selectCountDigdagPgLocks(@Bind("namespace_type") namespaceType: String,
                                 @Bind("namespace_value") namespaceValue: UUID,
                                 @Bind("name") name: String): Int

    @SqlQuery(
        "SELECT DISTINCT limit_count" +
            " FROM digdag_pg_locks" +
            " WHERE namespace_type = :namespace_type" +
            " AND namespace_value = :namespace_value" +
            " AND name = :name" +
            " AND expires_on >= now()")
    def selectDistinctLimitCountDigdagPgLocks(@Bind("namespace_type") namespaceType: String,
                                              @Bind("namespace_value") namespaceValue: UUID,
                                              @Bind("name") name: String): java.util.List[Integer]

    @SqlUpdate(
        "INSERT INTO digdag_pg_locks (" +
            " id, namespace_type, namespace_value, owner_attempt_id," +
            " name, limit_count, expires_on, updated_at, created_at" +
            " )" +
            " VALUES (" +
            " :id, :namespace_type, :namespace_value, :owner_attempt_id," +
            " :name, :limit_count, now() + :expire_in_seconds * INTERVAL '1' SECOND, now(), now()" +
            " )")
    def insertDigdagPgLock(@Bind("id") id: UUID,
                           @Bind("namespace_type") namespaceType: String,
                           @Bind("namespace_value") namespaceValue: UUID,
                           @Bind("owner_attempt_id") ownerAttemptId: Long,
                           @Bind("name") name: String,
                           @Bind("limit_count") limitCount: Int,
                           @Bind("expire_in_seconds") expireInSeconds: Long): Int

    @SqlUpdate("DELETE FROM digdag_pg_locks WHERE id = :id AND owner_attempt_id = :owner_attempt_id")
    def deleteOwnedDigdagPgLock(@Bind("id") id: UUID,
                                @Bind("owner_attempt_id") ownerAttemptId: Long): Unit
}
