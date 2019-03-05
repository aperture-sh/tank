package io.marauder.tank

import net.logstash.logback.marker.Markers.append
import net.logstash.logback.argument.StructuredArguments.value
import org.slf4j.Logger
import java.util.UUID

class Benchmark(private val LOG: Logger) {
    private val COR_ID = "correlation"
    private val START_TOKEN = value("order", "Start")
    private val END_TOKEN = value("order", "Finish")

    fun startLog(format: String, vararg arguments: Any): (x: String) -> Unit {
        val corrId = UUID.randomUUID().toString()

        LOG.info(append(COR_ID, corrId), "$START_TOKEN $format", *arguments)
        return fun (format: String) {
            LOG.info(append(COR_ID, corrId), "$END_TOKEN $format")
        }
    }

    fun startLogA(format: String, vararg arguments: Any): (x: String, Array<Any>) -> Unit {
        val corrId = UUID.randomUUID().toString()
        LOG.info(append(COR_ID, corrId), "$START_TOKEN $format", *arguments)
        return fun (format: String, vararg arguments: Any) {
            LOG.info(append(COR_ID, corrId), "$END_TOKEN $format", *arguments)
        }
    }

}