package io.marauder.tank

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.policies.RoundRobinPolicy
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
import io.marauder.supercharged.Clipper
import io.marauder.supercharged.Encoder
import io.marauder.supercharged.Projector
import io.marauder.supercharged.models.*
import io.marauder.tank.Tiler
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.json.JSON
import kotlinx.serialization.json.JsonParsingException
import kotlinx.serialization.parse
import kotlinx.serialization.parseList
import org.slf4j.LoggerFactory
import vector_tile.VectorTile
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.QueryOptions




fun main(args: Array<String>): Unit = io.ktor.server.netty.EngineMain.main(args)

    @InternalAPI
    @ImplicitReflectionSerializer
    fun Application.module() {

        val marker = Benchmark(LoggerFactory.getLogger(this::class.java))

        val minZoom = environment.config.propertyOrNull("ktor.application.min_zoom")?.getString()?.toInt() ?: 2
        val maxZoom = environment.config.propertyOrNull("ktor.application.max_zoom")?.getString()?.toInt() ?: 15
        val baseLayer = environment.config.propertyOrNull("ktor.application.base_layer")?.getString()
                ?: "io.marauder.tank"
        val extend = environment.config.propertyOrNull("ktor.application.extend")?.getString()?.toInt() ?: 4096
        val attrFields = environment.config.propertyOrNull("ktor.application.attr_field")?.getList() ?: emptyList()
        val buffer = environment.config.propertyOrNull("ktor.application.buffer")?.getString()?.toInt() ?: 64
        val dbHosts = environment.config.propertyOrNull("ktor.application.db_hosts")?.getString()?.split(",")?.map { it.trim() } ?: listOf("localhost")
        val dbStrategy = environment.config.propertyOrNull("ktor.application.db_strategy")?.getString() ?: "SimpleStrategy"
        val dbReplFactor = environment.config.propertyOrNull("ktor.application.replication_factor")?.getString()?.toInt() ?: 1

        val qo = QueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
        val clusterBuilder = Cluster.builder().apply {
            dbHosts.forEach {
                addContactPoint(it)
            }
            withLoadBalancingPolicy(RoundRobinPolicy())
            withQueryOptions(qo)
        }

        var isConnected = false
        var attempts = 10
        while (!isConnected && attempts >= 0) {
            try {
                initCassandra(clusterBuilder, dbStrategy, dbReplFactor)
                isConnected = true
            } catch (e: RuntimeException) {
                attempts--
                runBlocking {
                    delay(3_000)
                }
            }
        }

        val cluster = clusterBuilder.build()
        val session = cluster.connect("geo")
        val tiler = Tiler(session, minZoom, maxZoom, extend, buffer)
        val projector = Projector()

//        val q = session.prepare("SELECT geometry, id FROM features WHERE z=? AND x=? AND y=?;")

        val query = """SELECT geometry, id FROM features WHERE expr(test_idx, ?);""".trimMargin()

        println(query)

        val q = session.prepare(query)


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

                var endLog = marker.startLogDuration("prepare query")
                val z = call.parameters["z"]?.toInt()?:-1
                val x = call.parameters["x"]?.toInt()?:-1
                val y = call.parameters["y"]?.toInt()?:-1

                val box = projector.tileBBox(z, x, y)
//                println(box)

                val poly = Geometry.Polygon(coordinates = listOf(listOf(
                        listOf(box[0], box[1]),
                        listOf(box[2], box[1]),
                        listOf(box[2], box[3]),
                        listOf(box[0], box[3]),
                        listOf(box[0], box[1])
                )))

                val jsonQuery = """
                    {
                        filter: {
                         type: "geo_shape",
                         field: "geometry",
                         operation: "intersects",
                         shape: {
                            type: "wkt",
                            value: "${projector.projectFeature(Feature(geometry = poly)).geometry.toWKT()}"
                         }
                        }
                    }
                """.trimIndent()

//                println(jsonQuery)

                val bound = q.bind()
                        .setString(0, jsonQuery)

                endLog()

                endLog = marker.startLogDuration("CQL statement execution")
                val res = session.execute(bound)
                endLog()


                endLog = marker.startLogDuration("fetch features")
                val features = res.map { row ->
//                    println(row.getString(1))
                    Feature(
                            geometry = Geometry.fromWKT(row.getString(0))!!,
                            properties = mapOf("id" to Value.StringValue(row.getString(1)))
                    )

                }
                endLog()
                endLog = marker.startLogDuration("prepare features for encoding")
                val geojson = GeoJSON(features = features)
//                val tile = projector.transformTile(Tile(geojson, z, x, y))

                val z2 = 1 shl (if (z == 0) 0 else z)

                val k1 = 0.5 * buffer / extend
                val k3 = 1 + k1

                projector.calcBbox(geojson)

                val clipper = Clipper()
                val clipped = clipper.clip(geojson, z2.toDouble(), x - k1, x + k3, y - k1, y + k3)
                val tile = projector.transformTile(Tile(clipped, (1 shl z), x, y))

                val encoder = Encoder()

                endLog()
                endLog = marker.startLogDuration("encode and transmit")
                val encoded = encoder.encode(tile.geojson.features, baseLayer)

                call.respondBytes(encoded.toByteArray())
                endLog()
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

    private fun initCassandra(clusterBuilder: Cluster.Builder, s: String, r: Int): Boolean {
        val cluster = clusterBuilder.build()
        val session = cluster.connect()
        if (s == "SimpleStrategy") {
            session.execute("CREATE  KEYSPACE IF NOT EXISTS geo " +
                    "WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : $r };")
        } else {
            session.execute("CREATE  KEYSPACE IF NOT EXISTS geo " +
                    "WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'DC1' : $r};")
        }

        session.execute("USE geo;")
        session.execute("CREATE TABLE IF NOT EXISTS geo.features (    timestamp timestamp,    id text,    geometry text,    PRIMARY KEY (timestamp, id));")
        session.execute("CREATE CUSTOM INDEX IF NOT EXISTS test_idx ON geo.features (geometry) USING 'com.stratio.cassandra.lucene.Index' WITH OPTIONS = {'refresh_seconds': '1', 'schema': '{fields: { geometry: {type: \"geo_shape\", max_levels: 3, transformations: [{type: \"bbox\"}]}}}'}")
        session.close()
        cluster.close()
        return true
    }