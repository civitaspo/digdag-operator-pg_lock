package pro.civitaspo.digdag.plugin.pg_lock


import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File, PrintStream}
import java.nio.charset.StandardCharsets.UTF_8

import com.google.common.io.Files
import io.digdag.cli.Main
import io.digdag.client.DigdagVersion

import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.util.Using


object DigdagTestUtils
{
    case class CommandStatus(code: Int,
                             out: Array[Byte],
                             err: Array[Byte])
    {
        def getStdout: String =
        {
            new String(out, UTF_8)
        }

        def getStderr: String =
        {
            new String(err, UTF_8)
        }
    }

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
                CommandStatus(code, out.toByteArray, err.toByteArray)
            }
        }
    }
}
