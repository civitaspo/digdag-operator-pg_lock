package pro.civitaspo.digdag.plugin.pg_lock


import java.io.{ByteArrayOutputStream, File, PrintStream, StringReader}
import java.nio.charset.StandardCharsets.UTF_8
import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties

import com.google.common.io.Files
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, DiagrammedAssertions, FlatSpec}
import org.scalatestplus.junit.JUnitRunner
import pro.civitaspo.digdag.plugin.pg_lock.DigdagTestUtils.{digdagRun, readResource, CommandStatus}

import scala.util.Using
import scala.util.matching.Regex


@RunWith(classOf[JUnitRunner])
class PgLockPluginTest
    extends FlatSpec
        with BeforeAndAfter
        with DiagrammedAssertions
{
    val digdagPgLockProps: Properties = new Properties()
    var tmpDir: File = _

    def defaultSystemConfig: String =
    {
        Using.resource(new ByteArrayOutputStream) { b =>
            Using.resource(new PrintStream(b, true, "UTF-8")) { p =>
                digdagPgLockProps.store(p, "default")
                new String(b.toByteArray, UTF_8)
            }
        }
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
                stmt.execute( // Need to kill all processes to force dropping database
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
            configString = defaultSystemConfig,
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
                       |SELECT description
                       |  FROM pg_lock_schema_migrations
                       | WHERE version = '0.0.0'
                       | LIMIT 1
                """.stripMargin)

                val description = Iterator.continually(rs)
                    .takeWhile(_.next())
                    .map(_.getString("description"))
                    .toSeq
                    .head
                assert(description === "Baseline do nothing")
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

                assert(idxCols.contains("digdag_pg_locks_idx_owner_attempt_id"))
                assert(idxCols.getOrElse("digdag_pg_locks_idx_owner_attempt_id", Seq()) === Seq("owner_attempt_id"))
            }
        }

    }

    it should "migrate tables when digdag tables already exists" in {
        Using.resource(getJdbcPgLockConnection) { conn =>
            Using.resource(conn.createStatement()) { stmt =>
                stmt.executeUpdate("CREATE TABLE digdag_table (id integer)")
            }
        }
        val digString = readResource("/simple.dig")

        val status: CommandStatus = digdagRun(
            projectPath = tmpDir.toPath,
            configString = defaultSystemConfig,
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
                       |SELECT description
                       |  FROM pg_lock_schema_migrations
                       | WHERE version = '0.0.0'
                       | LIMIT 1
                """.stripMargin)

                val description = Iterator.continually(rs)
                    .takeWhile(_.next())
                    .map(_.getString("description"))
                    .toSeq
                    .head
                assert(description === "<< Flyway Baseline >>")
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

                assert(idxCols.contains("digdag_pg_locks_idx_owner_attempt_id"))
                assert(idxCols.getOrElse("digdag_pg_locks_idx_owner_attempt_id", Seq()) === Seq("owner_attempt_id"))
            }
        }
    }

    behavior of "the pg_lock> operator"
    it should "fail with wait_timeout: 0s if another task locks" in {
        val digString = readResource("/wait-timeout.dig")

        val status: CommandStatus = digdagRun(
            projectPath = tmpDir.toPath,
            configString = defaultSystemConfig,
            digString = digString
            )

        assert(status.code === 1)
        val expectedPattern: Regex = """\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \+\d{4} \[ERROR\] \(.+?\+main\+wait-timeout\+failure\+lock\): Task failed with unexpected error: Give up polling\.""".r
        assert(expectedPattern.findFirstIn(status.log.get).isDefined)
    }

    it should "lock up to the limit" in {
        val digString = readResource("/limit.dig")

        val status: CommandStatus = digdagRun(
            projectPath = tmpDir.toPath,
            configString = defaultSystemConfig,
            digString = digString
            )

        assert(status.code === 0)
        val expectedPattern: Regex = """\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \+\d{4} \[INFO\] \(.+?\+main\+limit\+lock-wait\+lock\): Wait because current lock count=2 reaches the limit=2\. \(namespace_type: site, namespace_value: .+?, name: lock\)""".r
        assert(expectedPattern.findFirstIn(status.log.get).isDefined)
    }

    it should "fail when several limit settings are defined about the same lock name" in {
        val digString = readResource("/conflict-limit.dig")

        val status: CommandStatus = digdagRun(
            projectPath = tmpDir.toPath,
            configString = defaultSystemConfig,
            digString = digString
            )

        assert(status.code === 1)
        val expectedPattern: Regex = """\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \+\d{4} \[ERROR\] \(.+?\+main\+conflict-limit\+limit-2\+lock\): Configuration error at task \+main\+conflict-limit\+limit-2\+lock: Conflict current config: limit=2 because another workflow defines limit=1\. \(config\)""".r
        assert(expectedPattern.findFirstIn(status.log.get).isDefined)
    }

    it should "fail when namespace is unknown" in {
        val digString = readResource("/namespace.dig")

        def assertNamespace(namespace: String,
                            expectError: Boolean = false): Unit =
        {
            val status: CommandStatus = digdagRun(
                projectPath = tmpDir.toPath,
                configString = defaultSystemConfig,
                digString = digString,
                params = Map("namespace" -> namespace)
                )

            if (expectError) {
                assert(status.code == 1)
                val expectedPattern: Regex = ("""\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \+\d{4} \[ERROR\] \(.+?\+main\+namespace\): Configuration error at task \+main\+namespace: Unsupported namespace: """ + namespace + """ \(config\)""").r
                assert(expectedPattern.findFirstIn(status.log.get).isDefined)
            }
            else {
                assert(status.code == 0)
                val expectedPattern: Regex = ("""\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \+\d{4} \[INFO\] \(.+?\+main\+namespace\): Successfully get the lock \(id: [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}, namespace_type: """ + namespace + """, namespace_value: [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}, owner_attempt_id: .+?, expire_in: .+?, limit: .+?\)""").r
                assert(expectedPattern.findFirstIn(status.log.get).isDefined)
            }
        }

        assertNamespace(namespace = "global")
        assertNamespace(namespace = "site")
        assertNamespace(namespace = "project")
        assertNamespace(namespace = "workflow")
        assertNamespace(namespace = "session")
        assertNamespace(namespace = "attempt")
        assertNamespace(namespace = "test", expectError = true)
    }

    it should "skip unlocking the finished attempt locks if unlock_finished_attempt_locks=false" in {
        val digString = readResource("/unlock-finished-attempt-locks.dig")

        val status: CommandStatus = digdagRun(
            projectPath = tmpDir.toPath,
            configString = defaultSystemConfig,
            digString = digString
            )

        assert(status.code === 0)
        assert(status.log.get.contains("Skip to release the other locks that other attempts are the owner of because unlock_finished_attempt_locks=false."))
    }
}
