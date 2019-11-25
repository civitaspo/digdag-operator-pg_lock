package pro.civitaspo.digdag.plugin.pg_lock.pg.migration

import org.flywaydb.core.api.migration.{BaseJavaMigration, Context}

import scala.util.Using


class V0_0_1_4__CreateIndex_digdag_pg_locks_idx_owner_attempt_id
    extends BaseJavaMigration
{
    override def migrate(context: Context): Unit =
    {
        Using(context.getConnection.prepareStatement(
            """
              |CREATE INDEX digdag_pg_locks_idx_owner_attempt_id
              | ON digdag_pg_locks(
              |     owner_attempt_id
              | )
            """.stripMargin)) { stmt =>

            stmt.execute()
        }
    }

}
