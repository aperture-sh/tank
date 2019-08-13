package io.marauder.tank

import io.ktor.client.HttpClient
import io.ktor.client.call.call
import io.ktor.client.engine.apache.Apache
import io.ktor.client.request.header
import io.ktor.client.response.readText
import io.ktor.http.HttpMethod
import io.marauder.charged.models.Feature
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.json.JSON
import kotlinx.serialization.stringify
import org.slf4j.LoggerFactory
import java.util.*

@ImplicitReflectionSerializer
class Exhauster(private val host: String, private val port: Int) {

    val http = HttpClient(Apache) {
        engine {
            socketTimeout = 300_000  // Max time between TCP packets - default 10 seconds
            connectTimeout = 60_000 // Max time to establish an HTTP connection - default 10 seconds
            connectionRequestTimeout = 300_000 // Max time for the connection manager to start a request - 20 seconds
        }

    }

    suspend fun pushFeature(uuid: UUID, f: Feature) {
        val tmp = Feature(
                id = uuid.toString(),
                geometry = f.geometry,
                properties = f.properties
        )
        val call = http.call(urlString = "http://$host:$port/") {
            method = HttpMethod.Post
            body = JSON.plain.stringify(tmp)
            header("ContentType", "application/json")
        }
        log.info("Fan out to Exhauster - ${call.response.readText()}")
    }

    companion object {
        private val log = LoggerFactory.getLogger(Exhauster::class.java)
    }
}