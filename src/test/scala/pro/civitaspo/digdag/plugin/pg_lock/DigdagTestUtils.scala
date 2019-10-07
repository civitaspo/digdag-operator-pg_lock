package pro.civitaspo.digdag.plugin.pg_lock


import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File, PrintStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Path

import com.google.common.io.Files
import io.digdag.cli.Main
import io.digdag.client.DigdagVersion

import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.util.Using


object DigdagTestUtils
{
    case class CommandStatus(code: Int,
                             stdout: String,
                             stderr: String,
                             log: Option[String] = None)


    def readResource(path: String): String =
    {
        Using.resource(classOf[PgLockPluginTest].getResourceAsStream(path)) { is =>
            Source.fromInputStream(is).mkString
        }
    }

    def writeFile(file: File,
                  str: String): Unit =
    {
        Files.write(str, file, UTF_8)
    }

    def digdag(args: String*): CommandStatus =
    {
        Using.resources(
            new ByteArrayOutputStream,
            new ByteArrayOutputStream,
            new ByteArrayInputStream(new Array[Byte](0))
            ) { (out,
                 err,
                 in
                ) =>
            Using.resources(
                new PrintStream(out, true, "UTF-8"),
                new PrintStream(err, true, "UTF-8")
                ) { (outPS: PrintStream,
                     errPS: PrintStream
                    ) =>
                val main = new Main(DigdagVersion.buildVersion(), Map[String, String]().asJava, outPS, errPS, in)
                val code: Int = main.cli(args: _*)
                CommandStatus(code, new String(out.toByteArray, UTF_8), new String(err.toByteArray, UTF_8))
            }
        }
    }

    def digdagRun(projectPath: Path,
                  configString: String,
                  digString: String,
                  params: Map[String, String] = Map()): CommandStatus =
    {
        val configPath: Path = projectPath.resolve("config")
        writeFile(configPath.toFile, configString)

        val digPath: Path = projectPath.resolve("main.dig")
        writeFile(digPath.toFile, digString)

        val logPath: Path = projectPath.resolve("log")

        val args: Seq[String] = Seq.newBuilder[String]
            .addOne("run")
            .addOne("--save").addOne(projectPath.toAbsolutePath.toString)
            .addOne("--config").addOne(configPath.toString)
            .addOne("--log").addOne(logPath.toString)
            .addOne("--project").addOne(projectPath.toAbsolutePath.toString)
            .addAll(params.toSeq.map(_.productIterator.mkString("=")).flatMap(Seq("--param", _)))
            .addOne(digPath.toString)
            .result()

        val status: CommandStatus = digdag(args: _*)

        val log: String = Source.fromFile(logPath.toFile).mkString
        status.copy(log = Option(log))
    }
}
