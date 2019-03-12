package io.marauder.tank

import org.slf4j.Logger
import java.util.UUID
import java.util.concurrent.TimeUnit

class Benchmark(private val LOG: Logger) {
    private val START_TOKEN = "Start"
    private val END_TOKEN = "Finish"

    fun startLog(format: String, vararg arguments: Any): (msg: String) -> Unit {
        val corrId = UUID.randomUUID().toString()

        LOG.info("$corrId - $START_TOKEN $format", *arguments)
        return fun (msg: String) {
            LOG.info("$corrId - $END_TOKEN $msg")
        }
    }

    fun startLogA(format: String, vararg arguments: Any): (x: String, Array<Any>) -> Unit {
        val corrId = UUID.randomUUID().toString()
        LOG.info("$corrId - $START_TOKEN $format", *arguments)
        return fun (format: String, vararg arguments: Any) {
            LOG.info("$corrId - $END_TOKEN $format", *arguments)
        }
    }

    fun startLogDuration(format: String, vararg arguments: Any): () -> Unit {
        val startTime = System.nanoTime()
        return fun () {
            val duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime)
            LOG.info("$duration ms - $format", *arguments)
        }
    }

}