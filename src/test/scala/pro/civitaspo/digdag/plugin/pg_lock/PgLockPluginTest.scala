package pro.civitaspo.digdag.plugin.pg_lock


import java.io.{File, StringReader}
import java.nio.file.Path
import java.sql.{Connection, DriverManager}
import java.util.Properties

import com.google.common.io.Files
import org.scalatest.{BeforeAndAfter, DiagrammedAssertions, FlatSpec, Matchers}
import pro.civitaspo.digdag.plugin.pg_lock.DigdagTestUtils.{readResource, writeFile, CommandStatus}

import scala.util.Using

class PgLockPluginTest
    extends FlatSpec
        with BeforeAndAfter
        with DiagrammedAssertions
        with Matchers
{
    val digdagPgLockProps: Properties = new Properties()
    var conn: Connection = _
    var tmpDir: File = _

    before {
        Using.resource(new StringReader(readResource("/digdag.properties"))) { reader =>
            digdagPgLockProps.load(reader)
        }

        val host: String = digdagPgLockProps.getProperty("pg_lock.host")
        val port: String = digdagPgLockProps.getProperty("pg_lock.port")
        val database: String = digdagPgLockProps.getProperty("pg_lock.database")
        val user: String = digdagPgLockProps.getProperty("pg_lock.user")
        val password: String = digdagPgLockProps.getProperty("pg_lock.password")
        conn = DriverManager.getConnection(s"jdbc:postgresql://$host:$port/postgres",
                                           user,
                                           password
                                           )

        // recreate database
        Using.resource(conn.createStatement()) { stmt =>
            stmt.executeUpdate(s"DROP DATABASE IF EXISTS $database")
            stmt.executeUpdate(s"CREATE DATABASE $database")
        }

        tmpDir = Files.createTempDir()
    }

    after {
        digdagPgLockProps.clear()

        conn.close()
        conn = null

        tmpDir.delete()
        tmpDir = null
    }


    behavior of "the pg_lock> operator properties setting"
    it should "require pg_lock.host" in {
        val projectPath: Path = tmpDir.toPath

        writeFile(projectPath.resolve("simple.dig").toFile, readResource("/simple.dig"))

        val configPath: Path = projectPath.resolve("config")
        writeFile(configPath.toFile,
                  s"""
                 |pg_lock.port=${digdagPgLockProps.getProperty("pg_lock.port")}
                 |pg_lock.database=${digdagPgLockProps.getProperty("pg_lock.database")}
                 |pg_lock.user=${digdagPgLockProps.getProperty("pg_lock.user")}
                 |pg_lock.password=${digdagPgLockProps.getProperty("pg_lock.password")}
            """.stripMargin)
        val status: CommandStatus = DigdagTestUtils.digdag(
            "run",
            "-o", projectPath.toAbsolutePath.toString,
            "--config", configPath.toString,
            "--project", projectPath.toString,
            projectPath.resolve("simple.dig").toString
            )

        status.code should be(1)
        status.getStderr should include("Parameter 'pg_lock.host' is required but not set")
    }
}
