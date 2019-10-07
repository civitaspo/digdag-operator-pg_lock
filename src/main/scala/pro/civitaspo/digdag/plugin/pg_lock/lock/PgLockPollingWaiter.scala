package pro.civitaspo.digdag.plugin.pg_lock.lock


import com.typesafe.scalalogging.LazyLogging
import io.digdag.client.config.{Config, ConfigElement, ConfigFactory}
import io.digdag.spi.TaskExecutionException
import io.digdag.util.DurationParam

import scala.util.chaining._


case class PgLockPollingWaiter(lastStateParams: Config,
                               cf: ConfigFactory,
                               waitTimeout: DurationParam,
                               pollInterval: DurationParam,
                               minPollInterval: DurationParam,
                               maxPollInterval: DurationParam)
    extends LazyLogging
{
    private val ROOT_STATE_PARAM_NAME: String = "pg_lock"
    private val RETRY_COUNT: String = "retry_count"
    private val TOTAL_WAITING_SECONDS: String = "total_waiting_seconds"

    class PollingGiveUpException(message: String = null,
                                 cause: Throwable = null)
        extends RuntimeException(message, cause)
    object PollingGiveUpException
    {
        def apply(message: String = null,
                  cause: Throwable = null): PollingGiveUpException =
        {
            new PollingGiveUpException(message, cause)
        }
    }

    private def buildPollingGiveUpException(retryCount: Int,
                                            totalWaitingSeconds: Int,
                                            ex: Throwable = null): PollingGiveUpException =
    {
        val msg: String = "Give up polling"
            .pipe { s =>
                if (ex == null) s + "."
                else s + s" because of ${ex.getMessage}"
            }
            .pipe { s =>
                s + s" (retry_count: $retryCount, total waiting: ${totalWaitingSeconds}s)"
            }
        PollingGiveUpException(message = msg, cause = ex)
    }

    def pollingWait(pollingName: String,
                    poll: => Boolean): Unit =
    {
        val lastRootState: Config = lastStateParams
            .getNestedOrSetEmpty(ROOT_STATE_PARAM_NAME)
        // NOTE: 'total_waiting_seconds' is used for all polling.
        val totalWaitingSeconds: Int = lastRootState // seconds
            .get(TOTAL_WAITING_SECONDS, classOf[Int], 0)

        val lastPollingState: Config = lastRootState
            .getNestedOrGetEmpty(pollingName)
        val retryCount: Int = lastPollingState
            .get(RETRY_COUNT, classOf[Int], 0)

        val isSuccess: Boolean =
            try poll
            catch {
                case ex: Throwable =>
                    throw buildPollingGiveUpException(retryCount, totalWaitingSeconds, ex)
            }

        if (isSuccess) {
            logger.debug(s"Successfully finished polling '$pollingName'. (retry_count: $retryCount, total waiting: ${totalWaitingSeconds}s)")

            // NOTE: Remove polling state for the next use of the PgLockPollingWaiter instance.
            lastRootState.remove(pollingName)

            return
        }

        if (totalWaitingSeconds > waitTimeout.getDuration.getSeconds) {
            throw buildPollingGiveUpException(retryCount, totalWaitingSeconds)
        }

        val baseSeconds = pollInterval.getDuration.getSeconds
        val minCapSeconds = minPollInterval.getDuration.getSeconds
        val maxCapSeconds = maxPollInterval.getDuration.getSeconds
        val nextWaitingSeconds: Int = math.max(minCapSeconds, math.random() * math.min(maxCapSeconds, baseSeconds * math.pow(2.0, retryCount))).toInt

        val nextStateParam: Config = cf.create()
            .tap { nextStateParam: Config =>
                nextStateParam
                    .getNestedOrSetEmpty(ROOT_STATE_PARAM_NAME)
                    .set(TOTAL_WAITING_SECONDS, totalWaitingSeconds + nextWaitingSeconds)
                    .getNestedOrSetEmpty(pollingName)
                    .tap { nextPollingState: Config =>
                        nextPollingState.set(RETRY_COUNT, retryCount + 1)
                    }
            }

        logger.debug(s"Wait ${nextWaitingSeconds}s for '$pollingName'. (next state => $nextStateParam)")
        throw TaskExecutionException.ofNextPolling(nextWaitingSeconds, ConfigElement.copyOf(nextStateParam))
    }
}
