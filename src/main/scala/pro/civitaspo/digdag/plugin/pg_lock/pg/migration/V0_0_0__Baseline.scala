package pro.civitaspo.digdag.plugin.pg_lock.pg.migration


import org.flywaydb.core.api.migration.{BaseJavaMigration, Context}

import scala.util.Using


class V0_0_0__Baseline
    extends BaseJavaMigration
{
    override def migrate(context: Context): Unit =
    {
        Using(context.getConnection.prepareStatement("SELECT 1")) { stmt =>
            stmt.execute()
        }

    }
}
