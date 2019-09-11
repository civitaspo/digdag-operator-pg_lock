package pro.civitaspo.digdag.plugin.pg_lock.migration

import org.flywaydb.core.api.migration.{BaseJavaMigration, Context}

import scala.util.Using


class V0_0_1_1__CreateTable_digdag_pg_locks extends BaseJavaMigration
{
    override def migrate(context: Context): Unit =
    {
        Using(context.getConnection.prepareStatement(
            """
              |CREATE TABLE digdag_pg_locks (
              |    id                UUID    NOT NULL,
              |    namespace_type    TEXT    NOT NULL,
              |    namespace_value   TEXT    NOT NULL,
              |    owner_site_id     INTEGER NOT NULL,
              |    owner_attempt_id  BIGINT  NOT NULL,
              |    name              TEXT    NOT NULL,
              |    max_count         INTEGER NOT NULL DEFAULT 1,
              |    expires_on        TIMESTAMP WITH TIME ZONE NOT NULL,
              |    updated_at        TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
              |    created_at        TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
              |
              |    CONSTRAINT digdag_pg_locks_pkey PRIMARY KEY (id)
              |)
            """.stripMargin)) { stmt =>

            stmt.execute()
        }
    }
}
