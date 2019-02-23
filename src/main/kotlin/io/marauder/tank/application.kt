package io.marauder.tank

import com.datastax.driver.core.Cluster
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
import io.ktor.util.InternalAPI
import io.ktor.util.decodeString
import io.marauder.supercharged.Projector
import io.marauder.supercharged.models.Feature
import io.marauder.supercharged.models.GeoJSON
import io.marauder.tank.tiling.Tiler
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.json.JSON
import kotlinx.serialization.json.JsonParsingException
import kotlinx.serialization.parse
import kotlinx.serialization.parseList
import vector_tile.VectorTile

fun main(args: Array<String>): Unit = io.ktor.server.netty.EngineMain.main(args)


    @InternalAPI
    @ImplicitReflectionSerializer
    fun Application.module() {

        val minZoom = environment.config.propertyOrNull("ktor.application.min_zoom")?.getString()?.toInt() ?: 2
        val maxZoom = environment.config.propertyOrNull("ktor.application.max_zoom")?.getString()?.toInt() ?: 15
        val baseLayer = environment.config.propertyOrNull("ktor.application.base_layer")?.getString() ?: "io.marauder.tank"
        val extend = environment.config.propertyOrNull("ktor.application.extend")?.getString()?.toInt() ?: 4096
        val attrFields = environment.config.propertyOrNull("ktor.application.attr_field")?.getList() ?: emptyList()
        val buffer = environment.config.propertyOrNull("ktor.application.buffer")?.getString()?.toInt() ?: 64
        val dbHost = environment.config.propertyOrNull("ktor.application.db_host")?.getString() ?: "localhost"
        val dbHosts = environment.config.propertyOrNull("ktor.application.db_hosts")?.getList() ?: emptyList()

        val cluster = Cluster.builder().apply {
            if (dbHosts.isNotEmpty()) {
                dbHosts.forEach {
                    addContactPoint(it)
                }
            } else {
                addContactPoint(dbHost)
            }
        }.build()

        val session = cluster.connect("geo")
        val tiler = Tiler(session, minZoom, maxZoom, extend, buffer)
        val projector = Projector()

        val q = session.prepare("SELECT geometry, id FROM features WHERE z=? AND x=? AND y=?;")


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

            post("/file") {
                val geojson = JSON.plain.parse<GeoJSON>(call.receiveText())
                GlobalScope.launch {
                    val neu = projector.projectFeatures(geojson)
                    tiler.tiler(neu)
                }


                call.respondText("Features Accepted", contentType = ContentType.Text.Plain, status = HttpStatusCode.Accepted)
            }

            post("/") {
                val projector = Projector()

                val features = mutableListOf<Feature>()
                call.receiveStream().bufferedReader().useLines { lines ->
                    lines.forEach { features.add(JSON.plain.parse(it)) }
                }
                val geojson = GeoJSON(features = features)
                GlobalScope.launch {
                    val neu = projector.projectFeatures(geojson)
                    tiler.tiler(neu)
                }

                call.respondText("Features Accepted", contentType = ContentType.Text.Plain, status = HttpStatusCode.Accepted)
            }

            get("/tile/{z}/{x}/{y}") {
                val bound = q.bind()
                        .setInt(0, call.parameters["z"]?.toInt()?:-1)
                        .setInt(1, call.parameters["x"]?.toInt()?:-1)
                        .setInt(2, call.parameters["y"]?.toInt()?:-1)

                val res = session.execute(bound)
                val layer = vector_tile.VectorTile.Tile.Layer.newBuilder()
                layer.name = baseLayer
                layer.version = 2

                res.forEach { row ->
                    val f = vector_tile.VectorTile.Tile.Feature.newBuilder()
                    val g = JSON.plain.parseList<Int>(row.getBytes(0).decodeString())
                    f.type = VectorTile.Tile.GeomType.POLYGON
                    f.addAllGeometry(g)
                    layer.addFeatures(f.build())
                }
                val tile = vector_tile.VectorTile.Tile.newBuilder()
                tile.addLayers(layer.build())

                call.respondBytes(tile.build().toByteArray())
            }

            static("/static") {
                resources("static")
            }

            install(StatusPages) {
                exception<OutOfMemoryError> {
                    call.respond(status = HttpStatusCode.InternalServerError, message = "Out of memory: reduce file/bulk size")
                }

                exception<JsonParsingException> {
                    call.respond(status = HttpStatusCode.InternalServerError, message = "Json Parsing Issue: Check file format")
                }

            }
        }
    }
