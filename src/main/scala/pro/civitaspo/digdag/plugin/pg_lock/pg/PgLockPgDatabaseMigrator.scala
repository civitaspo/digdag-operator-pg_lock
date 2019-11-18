package pro.civitaspo.digdag.plugin.pg_lock.pg


import javax.sql.DataSource
import org.flywaydb.core.Flyway
import pro.civitaspo.digdag.plugin.pg_lock.pg.migration.{V0_0_0__Baseline_do_nothing, V0_0_1_1__CreateTable_digdag_pg_locks, V0_0_1_2__CreateIndex_digdag_pg_locks_idx_expires_on, V0_0_1_3__CreateIndex_digdag_pg_locks_idx_named_lock, V0_0_1_4__CreateIndex_digdag_pg_locks_idx_owner_attempt_id}


case class PgLockPgDatabaseMigrator(config: PgLockPgConfig,
                                    ds: DataSource)
{
    private val v0_0_0__Baseline = new V0_0_0__Baseline_do_nothing()
    private val v0_0_1_1__CreateTable_digdag_pg_locks = new V0_0_1_1__CreateTable_digdag_pg_locks()
    private val v0_0_1_2__CreateIndex_digdag_pg_locks_idx_expires_on = new V0_0_1_2__CreateIndex_digdag_pg_locks_idx_expires_on()
    private val v0_0_1_3__CreateIndex_digdag_pg_locks_idx_named_lock = new V0_0_1_3__CreateIndex_digdag_pg_locks_idx_named_lock()
    private val v0_0_1_4__CreateIndex_digdag_pg_locks_idx_owner_attempt_id = new V0_0_1_4__CreateIndex_digdag_pg_locks_idx_owner_attempt_id()

    def migrate(): Unit =
    {
        Flyway.configure()
            .baselineOnMigrate(true)
            .dataSource(ds)
            .table(config.schemaMigrationHistoryTable)
            .baselineVersion(v0_0_0__Baseline.getVersion)
            .javaMigrations(
                v0_0_0__Baseline,
                v0_0_1_1__CreateTable_digdag_pg_locks,
                v0_0_1_2__CreateIndex_digdag_pg_locks_idx_expires_on,
                v0_0_1_3__CreateIndex_digdag_pg_locks_idx_named_lock,
                v0_0_1_4__CreateIndex_digdag_pg_locks_idx_owner_attempt_id
                )
            .load()
            .migrate()
    }
}
