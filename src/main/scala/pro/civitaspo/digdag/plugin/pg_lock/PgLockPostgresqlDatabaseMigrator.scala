package pro.civitaspo.digdag.plugin.pg_lock


import javax.sql.DataSource
import org.flywaydb.core.Flyway
import pro.civitaspo.digdag.plugin.pg_lock.migration.{V0_0_1_1__CreateTable_digdag_pg_locks, V0_0_1_2__CreateIndex_digdag_pg_locks_idx_expires_on, V0_0_1_3__CreateIndex_digdag_pg_locks_idx_named_lock}


case class PgLockPostgresqlDatabaseMigrator(ds: DataSource)
{
    def migrate(): Unit =
    {
        Flyway.configure()
            .dataSource(ds)
            .table("pg_lock_schema_migrations")
            .javaMigrations(
                new V0_0_1_1__CreateTable_digdag_pg_locks(),
                new V0_0_1_2__CreateIndex_digdag_pg_locks_idx_expires_on(),
                new V0_0_1_3__CreateIndex_digdag_pg_locks_idx_named_lock()
                )
            .load()
            .migrate()
    }
}
