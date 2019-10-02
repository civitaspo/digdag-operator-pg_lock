package pro.civitaspo.digdag.plugin.pg_lock


import java.io.{File, StringReader}
import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties

import com.google.common.io.Files
import org.scalatest.{BeforeAndAfter, DiagrammedAssertions, FlatSpec}
import pro.civitaspo.digdag.plugin.pg_lock.DigdagTestUtils.{digdagRun, readResource, CommandStatus}

import scala.util.Using


class PgLockPluginTest
    extends FlatSpec
        with BeforeAndAfter
        with DiagrammedAssertions
{
    val digdagPgLockProps: Properties = new Properties()
    var tmpDir: File = _

    def minimumSystemConfig: String =
    {
        s"""
           |pg_lock.host=${p("host")}
           |pg_lock.database=${p("database")}
           |pg_lock.user=${p("user")}
           |pg_lock.password=${p("password")}
         """.stripMargin
    }

    def getJdbcPostgresConnection: Connection =
    {
        DriverManager.getConnection(s"jdbc:postgresql://${p("host")}:${p("port")}/postgres",
                                    p("user"),
                                    p("password")
                                    )
    }

    def getJdbcPgLockConnection: Connection =
    {
        DriverManager.getConnection(s"jdbc:postgresql://${p("host")}:${p("port")}/${p("database")}",
                                    p("user"),
                                    p("password")
                                    )
    }

    def p(name: String): String =
    {
        digdagPgLockProps.getProperty(s"pg_lock.$name")
    }

    before {
        Using.resource(new StringReader(readResource("/digdag.properties"))) { reader =>
            digdagPgLockProps.load(reader)
        }
        Using.resource(getJdbcPostgresConnection) { conn =>
            // recreate database
            Using.resource(conn.createStatement()) { stmt =>
                stmt.execute(  // Need to kill all processes to force dropping database
                    s"""
                       |SELECT pg_terminate_backend(pg_stat_activity.pid)
                       |  FROM pg_stat_activity
                       | WHERE pg_stat_activity.datname = '${p("database")}'
                       |   AND pid <> pg_backend_pid()
                     """.stripMargin)
                stmt.executeUpdate(s"DROP DATABASE IF EXISTS ${p("database")}")
                stmt.executeUpdate(s"CREATE DATABASE ${p("database")}")
            }
        }

        tmpDir = Files.createTempDir()
    }

    after {
        digdagPgLockProps.clear()

        tmpDir.delete()
        tmpDir = null
    }


    behavior of "the pg_lock> operator properties setting"
    it should "require pg_lock.host" in {
        val configString =
            s"""
               |pg_lock.database=${p("database")}
               |pg_lock.user=${p("user")}
               |pg_lock.password=${p("password")}
             """.stripMargin

        val digString = readResource("/simple.dig")

        val status: CommandStatus = digdagRun(
            projectPath = tmpDir.toPath,
            configString = configString,
            digString = digString
            )

        assert(status.code === 1)
        assert(status.stderr.contains("Parameter 'pg_lock.host' is required but not set"))
    }

    it should "require pg_lock.database" in {
        val configString =
            s"""
               |pg_lock.host=${p("host")}
               |pg_lock.user=${p("user")}
               |pg_lock.password=${p("password")}
             """.stripMargin

        val digString = readResource("/simple.dig")

        val status: CommandStatus = digdagRun(
            projectPath = tmpDir.toPath,
            configString = configString,
            digString = digString
            )

        assert(status.code === 1)
        assert(status.stderr.contains("Parameter 'pg_lock.database' is required but not set"))
    }

    it should "require pg_lock.user" in {
        val configString =
            s"""
               |pg_lock.host=${p("host")}
               |pg_lock.database=${p("database")}
               |pg_lock.password=${p("password")}
             """.stripMargin

        val digString = readResource("/simple.dig")

        val status: CommandStatus = digdagRun(
            projectPath = tmpDir.toPath,
            configString = configString,
            digString = digString
            )

        assert(status.code === 1)
        assert(status.stderr.contains("Parameter 'pg_lock.user' is required but not set"))
    }

    it should "require pg_lock.password" in {
        val configString =
            s"""
               |pg_lock.host=${p("host")}
               |pg_lock.database=${p("database")}
               |pg_lock.user=${p("user")}
             """.stripMargin

        val digString = readResource("/simple.dig")

        val status: CommandStatus = digdagRun(
            projectPath = tmpDir.toPath,
            configString = configString,
            digString = digString
            )

        assert(status.code === 1)
        assert(status.stderr.contains("The server requested password-based authentication, but no password was provided."))
    }

    it should "migrate tables" in {
        val digString = readResource("/simple.dig")

        val status: CommandStatus = digdagRun(
            projectPath = tmpDir.toPath,
            configString = minimumSystemConfig,
            digString = digString
            )

        assert(status.code === 0)

        Using.resource(getJdbcPgLockConnection) { conn =>
            Using.resource(conn.createStatement()) { stmt =>
                val rs: ResultSet = stmt.executeQuery(
                    s"""
                       |SELECT table_name
                       |  FROM information_schema.tables
                       | WHERE table_catalog = '${p("database")}'
                       |   AND table_name IN ('digdag_pg_locks', 'pg_lock_schema_migrations')
                       | ORDER
                       |    BY 1
                """.stripMargin)
                val tableNames: Seq[String] = Iterator.continually(rs)
                    .takeWhile(_.next())
                    .map(_.getString("table_name"))
                    .toSeq

                assert(tableNames.size === 2)
                assert(tableNames(0) === "digdag_pg_locks")
                assert(tableNames(1) === "pg_lock_schema_migrations")
            }
        }

        Using.resource(getJdbcPgLockConnection) { conn =>
            Using.resource(conn.createStatement()) { stmt =>
                val rs: ResultSet = stmt.executeQuery(
                    s"""
                       |   SELECT db.datname AS database_name
                       |        , t.relname  AS table_name
                       |        , i.relname  AS index_name
                       |        , a.attname  AS column_name
                       |        , a.attnum   AS column_pos
                       |     FROM pg_class t
                       |        , pg_class i
                       |        , pg_index ix
                       |        , pg_attribute a
                       |        , pg_database db
                       |    WHERE t.oid      = ix.indrelid
                       |      AND i.oid      = ix.indexrelid
                       |      AND t.relowner = db.datdba
                       |      AND a.attrelid = t.oid
                       |      AND a.attnum   = ANY(ix.indkey)
                       |      AND t.relkind  = 'r'
                       |      AND t.relname  = 'digdag_pg_locks'
                       |      AND db.datname = 'digdag'
                       | ORDER BY t.relname
                       |        , i.relname
                       |        , a.attnum
                     """.stripMargin)

                val idxCols: Map[String, Seq[String]] = Iterator.continually(rs)
                    .takeWhile(_.next())
                    .foldLeft(Map[String, Seq[String]]()) { (result,
                                                             rs) =>
                        val idxName: String = rs.getString("index_name")
                        val cols: Seq[String] = result.getOrElse(idxName, Seq()) :+ rs.getString("column_name")
                        result.updated(idxName, cols)
                    }

                assert(idxCols.contains("digdag_pg_locks_pkey"))
                assert(idxCols.getOrElse("digdag_pg_locks_pkey", Seq()) === Seq("id"))

                assert(idxCols.contains("digdag_pg_locks_idx_named_locks"))
                assert(idxCols.getOrElse("digdag_pg_locks_idx_named_locks", Seq()) === Seq("namespace_type", "namespace_value", "name"))

                assert(idxCols.contains("digdag_pg_locks_idx_expires_on"))
                assert(idxCols.getOrElse("digdag_pg_locks_idx_expires_on", Seq()) === Seq("expires_on"))
            }
        }

    }

    behavior of "the pg_lock> operator"
    it should "run" in {
        val digString = readResource("/simple.dig")

        val status: CommandStatus = digdagRun(
            projectPath = tmpDir.toPath,
            configString = minimumSystemConfig,
            digString = digString
            )

        assert(status.code === 0)
    }
}
