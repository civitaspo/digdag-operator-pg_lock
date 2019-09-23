package pro.civitaspo.digdag.plugin.pg_lock.lock


import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import io.digdag.client.config.{Config, ConfigException, ConfigFactory}
import io.digdag.spi.{OperatorContext, TaskResult}
import io.digdag.util.{BaseOperator, DurationParam}
import pro.civitaspo.digdag.plugin.pg_lock.pg.{PgLockPgClient, PgLockPgDao}

import scala.jdk.CollectionConverters._
import scala.util.chaining._
import scala.util.hashing.MurmurHash3


class PgLockOperator(context: OperatorContext,
                     systemConfig: Config,
                     pgClient: PgLockPgClient)
    extends BaseOperator(context)
        with LazyLogging
{
    override def runTask(): TaskResult =
    {
        try OperatorRunner().run()
        finally pgClient.close()
    }

    private case class OperatorRunner()
    {
        protected def params: Config =
        {
            request.getConfig
        }

        protected def parseNamespace(ns: String): PgLockNamespace =
        {
            PgLockNamespace.Factory(request).create(ns)
        }

        protected val name: String = params
            .get("_command", classOf[String])
        protected val waitTimeout: DurationParam = params
            .get("wait_timeout", classOf[DurationParam], DurationParam.parse("15m"))
        protected val expireIn: DurationParam = params
            .get("expire_in", classOf[DurationParam], DurationParam.parse("1h"))
        protected val limit: Int = params
            .get("limit", classOf[Int], 1)
        protected val namespace: PgLockNamespace = params
            .get("namespace", classOf[String], "site")
            .pipe(parseNamespace)
        protected val doConfig: Config = params
            .get("_do", classOf[Config])

        protected val hashSeedForAdvisoryLock: Int = systemConfig
            .get(s"pg_lock.hash_seed_for_advisory_lock", classOf[Int], -137723950)
        protected val pollInterval: DurationParam = systemConfig
            .get(s"pg_lock.poll_interval", classOf[DurationParam], DurationParam.parse("5s"))
        protected val minPollInterval: DurationParam = systemConfig
            .get(s"pg_lock.min_poll_interval", classOf[DurationParam], DurationParam.parse("5s"))
        protected val maxPollInterval: DurationParam = systemConfig
            .get(s"pg_lock.max_poll_interval", classOf[DurationParam], DurationParam.parse("5m"))

        protected val cf: ConfigFactory = request.getConfig.getFactory
        protected val pollingWaiter: PgLockPollingWaiter = PgLockPollingWaiter(
            lastStateParams = request.getLastStateParams,
            cf = cf,
            waitTimeout = waitTimeout,
            pollInterval = pollInterval,
            minPollInterval = minPollInterval,
            maxPollInterval = maxPollInterval
            )

        def run(): TaskResult =
        {
            pgClient.transaction { dao =>
                withAdvisoryLock(dao) {
                    releaseExpiredLocks(dao)
                    validateLockLimit(dao)
                    waitUntilAffordingLockableCount(dao)

                    val lockId: UUID = UUID.randomUUID()
                    doLock(dao, lockId)
                    pgClient.commit()

                    buildTaskResult(lockId)
                }
            }
        }

        protected def hash(str: String): Int =
        {
            MurmurHash3.stringHash(str, hashSeedForAdvisoryLock)
        }

        protected def withAdvisoryLock[A](dao: PgLockPgDao)
                                         (f: => A): A =
        {
            val k1: Int = hash(s"${namespace.getType}:${namespace.getValue}")
            val k2: Int = hash(name)

            pollingWaiter.pollingWait("advisoryLock", dao.pgTryAdvisoryLock(k1, k2))

            try f
            finally {
                try {
                    if (!dao.pgAdvisoryUnlock(k1, k2)) {
                        logger.warn(s"'pg_advisory_unlock($k1, $k2)' is failed.")
                    }
                }
                catch {
                    case ex: Throwable =>
                        logger.warn(
                            s"'pg_advisory_unlock($k1, $k2)' is failed because of ${ex.getMessage}",
                            ex
                            )
                }
            }
        }

        protected def releaseExpiredLocks(dao: PgLockPgDao): Unit =
        {
            dao.deleteExpiredDigdagPgLocks(
                namespaceType = namespace.getType,
                namespaceValue = namespace.getValue,
                name = name
                )
        }

        protected def validateLockLimit(dao: PgLockPgDao): Unit =
        {
            dao.selectDistinctLimitCountDigdagPgLocks(
                namespaceType = namespace.getType,
                namespaceValue = namespace.getValue,
                name = name
                )
                .asScala
                .foreach { i =>
                    if (i != limit) {
                        throw new ConfigException(
                            s"Conflict current config: limit=$limit" +
                                s" because another workflow defines limit=$i."
                            )
                    }
                }
        }

        protected def waitUntilAffordingLockableCount(dao: PgLockPgDao): Unit =
        {
            pollingWaiter.pollingWait("affordLockableCount", {
                val cnt: Int = dao.selectCountDigdagPgLocks(
                    namespaceType = namespace.getType,
                    namespaceValue = namespace.getValue,
                    name = name
                    )
                ((cnt + 1) <= limit).tap { isLockable =>
                    if (!isLockable) {
                        logger.info(
                            s"Wait because current lock count=$cnt reaches the limit=$limit." +
                                s" (namespace_type: ${namespace.getType}," +
                                s" namespace_value: ${namespace.getValue}, name: $name)"
                            )
                    }
                }
            })
        }

        protected def doLock(dao: PgLockPgDao,
                             lockId: UUID): Unit =
        {
            dao.insertDigdagPgLock(
                id = lockId,
                namespaceType = namespace.getType,
                namespaceValue = namespace.getValue,
                ownerAttemptId = namespace.getOwnerAttemptId,
                name = name,
                limitCount = limit,
                expireInSeconds = expireIn.getDuration.getSeconds
                )
        }

        protected def buildTaskResult(lockId: UUID): TaskResult =
        {
            TaskResult.defaultBuilder(request)
                .subtaskConfig(
                    cf.create()
                        .setNested("_error", buildSubtaskPgUnlock(lockId))
                        .setNested("+do", doConfig)
                        .setNested("+finally", buildSubtaskPgUnlock(lockId))
                    )
                .build()
        }

        protected def buildSubtaskPgUnlock(lockId: UUID): Config =
        {
            cf.create().tap { subtask =>
                subtask
                    .set("_type", "pg_unlock")
                    .set("_command", lockId)
                    .getNestedOrSetEmpty("_retry")
                    // TODO: configurable?
                    .set("limit", 30)
                    .set("interval", 1)
                    .set("max_interval", 10)
                    .set("interval_type", "exponential")
            }
        }
    }
}
