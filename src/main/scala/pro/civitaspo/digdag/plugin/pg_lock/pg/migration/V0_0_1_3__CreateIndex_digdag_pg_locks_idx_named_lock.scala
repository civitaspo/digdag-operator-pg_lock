package pro.civitaspo.digdag.plugin.pg_lock.pg.migration


import org.flywaydb.core.api.migration.{BaseJavaMigration, Context}

import scala.util.Using


class V0_0_1_3__CreateIndex_digdag_pg_locks_idx_named_lock
    extends BaseJavaMigration
{
    override def migrate(context: Context): Unit =
    {
        Using(context.getConnection.prepareStatement(
            """
              |CREATE INDEX digdag_pg_locks_idx_named_locks
              | ON digdag_pg_locks(
              |     namespace_type,
              |     namespace_value,
              |     name
              | )
            """.stripMargin)) { stmt =>

            stmt.execute()
        }
    }

}
