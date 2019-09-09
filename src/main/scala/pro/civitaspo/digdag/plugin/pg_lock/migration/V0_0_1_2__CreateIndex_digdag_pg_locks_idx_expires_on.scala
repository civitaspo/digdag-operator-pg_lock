package pro.civitaspo.digdag.plugin.pg_lock.migration

import org.flywaydb.core.api.migration.{BaseJavaMigration, Context}

import scala.util.Using


class V0_0_1_2__CreateIndex_digdag_pg_locks_idx_expires_on
    extends BaseJavaMigration
{
    override def migrate(context: Context): Unit =
    {
        Using(context.getConnection.prepareStatement(
            """
              |CREATE INDEX digdag_pg_locks_idx_expires_on
              | ON digdag_pg_locks(
              |     expires_on
              | )
            """.stripMargin)) { stmt =>

            stmt.execute()
        }
    }
}
