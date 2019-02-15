package io.marauder.tank

import io.ktor.application.*
import io.ktor.response.*
import io.ktor.request.*
import io.ktor.routing.*
import io.ktor.http.*
import io.ktor.features.*
import org.slf4j.event.*
import java.time.*
import io.ktor.http.content.resources
import io.ktor.http.content.static
import kotlinx.serialization.json.JsonParsingException

fun main(args: Array<String>): Unit = io.ktor.server.netty.EngineMain.main(args)

    fun Application.module() {

        val baseLayer = environment.config.propertyOrNull("ktor.application.base_layer")?.getString() ?: "io.marauder.tyler"
        val extend = environment.config.propertyOrNull("ktor.application.extend")?.getString()?.toInt() ?: 4096
        val buffer = environment.config.propertyOrNull("ktor.application.buffer")?.getString()?.toInt() ?: 64

        install(Compression) {
            gzip {
                priority = 1.0
            }
            deflate {
                priority = 10.0
                minimumSize(1024) // condition
            }
        }

        install(CallLogging) {
            level = Level.INFO
            filter { call -> call.request.path().startsWith("/") }
        }

        install(DefaultHeaders) {
            header("X-Engine", "Ktor") // will send this header with each response
        }

        install(io.ktor.websocket.WebSockets) {
            pingPeriod = Duration.ofSeconds(15)
            timeout = Duration.ofSeconds(15)
            maxFrameSize = Long.MAX_VALUE
            masking = false
        }

        routing {
            get("/") {
                call.respondText("Tank Tyle Database is running")
            }

            post("/") {
                call.respondText("Features Accepted", contentType = ContentType.Text.Plain, status = HttpStatusCode.Accepted)
            }

            static("/static") {
                resources("static")
            }

            install(StatusPages) {
                exception<OutOfMemoryError> {
                    call.respond(status = HttpStatusCode.InternalServerError, message = "Out of memory: reduce file size")
                }

                exception<JsonParsingException> {
                    call.respond(status = HttpStatusCode.InternalServerError, message = "Json Parsing Issue: Check file format")
                }

            }
        }
    }
