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
import io.marauder.charged.Clipper
import io.marauder.charged.Encoder
import io.marauder.charged.Projector
import io.marauder.charged.models.*
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.json.JSON
import kotlinx.serialization.json.JsonParsingException
import kotlinx.serialization.parse
import org.slf4j.LoggerFactory
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.LocalDate
import com.datastax.driver.core.QueryOptions
import com.google.gson.Gson
import io.ktor.util.KtorExperimentalAPI
import java.io.File
import java.util.UUID


fun main(args: Array<String>): Unit = io.ktor.server.netty.EngineMain.main(args)


    @KtorExperimentalAPI
    @InternalAPI
    @ImplicitReflectionSerializer
    fun Application.module() {

        val marker = Benchmark(LoggerFactory.getLogger(this::class.java))

        val tmpDirectory = environment.config.propertyOrNull("ktor.application.tmp_dir")?.getString()
                ?: "./tmp"

        val baseLayer = environment.config.propertyOrNull("ktor.application.tyler.base_layer")?.getString()
                ?: "io.marauder.tank"
        val extend = environment.config.propertyOrNull("ktor.application.tyler.extend")?.getString()?.toInt() ?: 4096
        val mainAttr = environment.config.propertyOrNull("ktor.application.tyler.main_attr")?.getString() ?: ""
        val mainAttrDefault = environment.config.propertyOrNull("ktor.application.tyler.main_attr_default")?.getString() ?: ""
        val attributes = environment.config.propertyOrNull("ktor.application.tyler.attributes")?.getString()?.let { if (it == "") null else it }?.split(",")?.map { it.trim() } ?: listOf()
        val buffer = environment.config.propertyOrNull("ktor.application.tyler.buffer")?.getString()?.toInt() ?: 64

        val dbHosts = environment.config.propertyOrNull("ktor.application.db.hosts")?.getString()?.split(",")?.map { it.trim() } ?: listOf("localhost")
        val dbUser = environment.config.propertyOrNull("ktor.application.db.user")?.getString() ?: ""
        val dbPassword = environment.config.propertyOrNull("ktor.application.db.password")?.getString() ?: ""
        val dbDatacenter = environment.config.propertyOrNull("ktor.application.db.datacenter")?.getString() ?: "datacenter1"
        val dbStrategy = environment.config.propertyOrNull("ktor.application.db.strategy")?.getString() ?: "SimpleStrategy"
        val dbKeyspace = environment.config.propertyOrNull("ktor.application.db.keyspace")?.getString() ?: "geo"
        val dbGeoIndex = environment.config.propertyOrNull("ktor.application.db.geo_index")?.getString() ?: "geo_index"
        val dbTable = environment.config.propertyOrNull("ktor.application.db.table")?.getString() ?: "features"
        val dbReplFactor = environment.config.propertyOrNull("ktor.application.db.replication_factor")?.getString()?.toInt() ?: 1

        val partitionKeys = environment.config.propertyOrNull("ktor.application.data.partition_keys")?.getString()?.let { if (it == "") null else it }?.split(",")?.map { it.trim() } ?: listOf("timestamp")
        val primaryKeys = environment.config.propertyOrNull("ktor.application.data.primary_keys")?.getString()?.let { if (it == "") null else it }?.split(",")?.map { it.trim() } ?: listOf()
        val attrFields = environment.config.propertyOrNull("ktor.application.data.attr_fields")?.getString()?.let { if (it == "") null else it }?.split(",")?.map { it.trim() } ?: listOf("timestamp")
        val addTimeStamp = environment.config.propertyOrNull("ktor.application.data.add_timestamp")?.getString()?.let { it == "true" } ?: true

        File(tmpDirectory).mkdirs()

        val qo = QueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_ONE)
        val clusterBuilder = Cluster.builder().apply {
            if (dbUser != "") {
                withCredentials(dbUser, dbPassword)
            }
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
                initCassandra(clusterBuilder, dbStrategy, dbReplFactor, dbKeyspace, dbGeoIndex, dbTable, dbDatacenter, partitionKeys, primaryKeys, attrFields)
                isConnected = true
            } catch (e: RuntimeException) {
                e.printStackTrace()
                attempts--
                runBlocking {
                    delay(10_000)
                }
            }
        }

        val cluster = clusterBuilder.build()
        val session = cluster.connect(dbKeyspace)
        val tiler = Tyler(session, dbTable, addTimeStamp, attrFields)
        val projector = Projector()

        val query = """
            | SELECT geometry${if (attributes.isNotEmpty()) attributes.joinToString(",", ",") else "" }
            | FROM $dbTable
            | WHERE geohash_data = :hash AND geohash_heatmap = :hash2 AND ${ if (mainAttr != "") "$mainAttr = :main AND" else "" } expr($dbGeoIndex, :json);
            | """.trimMargin()

        val hugeQuery = """
            | SELECT geometry${if (attributes.isNotEmpty()) attributes.joinToString(",", ",") else "" }
            | FROM $dbTable
            | WHERE geohash_data = :hash AND geohash_heatmap = :hash2 AND expr($dbGeoIndex, :json);
            | """.trimMargin()

        val countQuery = """
            | SELECT count(timestamp) AS count
            | FROM $dbTable
            | WHERE geohash_data = :hash AND geohash_heatmap = :hash2 AND expr($dbGeoIndex, :json);
            | """.trimMargin()

        val q = session.prepare(query)
        val qHuge = session.prepare(hugeQuery)
        val qHeatmap = session.prepare(countQuery)

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
                call.respondText("Tank is running and the endpoints are available")
            }

            post("/{layer?}") {
                val importLayer = call.parameters["layer"] ?: ""
                if (baseLayer == "" && importLayer == "") {
                    call.respondText("Import layer must not be an empty string", status = HttpStatusCode.BadRequest)
                } else {
                    val layer = "$baseLayer${if (baseLayer != "" && importLayer != "") "." else ""}$importLayer"
                    val importId = UUID.randomUUID()
                    val importFile = File("$tmpDirectory/$importId")
                    call.receiveStream().copyTo(importFile.outputStream())
                    if (call.parameters["geojson"] == "true") {
                        GlobalScope.launch {
                            val input = JSON.plain.parse<GeoJSON>(importFile.readText())
                            tiler.import(input)
                            importFile.delete()
                        }
                    } else {
                        GlobalScope.launch {
                            importFile.bufferedReader().useLines { lines ->
                                lines.chunked(1000).forEach { chunk ->
                                    val features = mutableListOf<Feature>()
                                    chunk.forEach { features.add(JSON.plain.parse(it)) }
                                    tiler.import(GeoJSON(features = features))
                                }
                            }
                            importFile.delete()
                        }
                    }

                    call.respondText("Features Accepted", contentType = ContentType.Text.Plain, status = HttpStatusCode.Accepted)
                }
            }

            get("/tile/{z}/{x}/{y}") {

                var endLog = marker.startLogDuration("prepare query")
                val z = call.parameters["z"]?.toInt()?:-1
                val x = call.parameters["x"]?.toInt()?:-1
                val y = call.parameters["y"]?.toInt()?:-1

                val gson = Gson()

                val typeMap = attrFields.map { attr ->
                    val (name, type) = attr.split(" ")
                    name to type
                }.toMap()

                val filters = gson.fromJson<Map<String,Any>>(call.parameters["filter"]?:"{}", Map::class.java)

                val mainFilter = (filters[mainAttr] ?: mainAttrDefault).toString()

                val box = projector.tileBBox(z, x, y)

                val poly = Geometry.Polygon(coordinates = listOf(listOf(
                        listOf(box[0], box[1]),
                        listOf(box[2], box[1]),
                        listOf(box[2], box[3]),
                        listOf(box[0], box[3]),
                        listOf(box[0], box[1])
                )))

                val f = Feature(geometry = poly)
                val centroid = f.geometry.toJTS().centroid
                val tileNumber1 = projector.getTileNumber(centroid.y, centroid.x, 5)
                val hash1 = GeoHashUtils.encode(
                        projector.tileToLat(tileNumber1.third, 5),
                        projector.tileToLon(tileNumber1.second, 5))

                val tileNumber2 = projector.getTileNumber(centroid.y, centroid.x, 10)
                val hash2 = GeoHashUtils.encode(
                        projector.tileToLat(tileNumber2.third, 10),
                        projector.tileToLon(tileNumber2.second, 10))

                val jsonQuery = """
                    {
                        filter: {
                         type: "geo_shape",
                         field: "geometry",
                         operation: "intersects",
                         shape: {
                            type: "wkt",
                            value: "${Feature(geometry = poly).geometry.toWKT()}"
                         }
                        }
                    }
                """.trimIndent()

                val bound = if (mainFilter == "*") {
                    qHuge.bind().setString("json", jsonQuery)
                } else {
                    q.bind().setString("json", jsonQuery)
                }

                bound.setString("hash", hash1)
                bound.setString("hash2", hash2)

                if (mainAttr !in listOf("", "*") && mainFilter != "") {
                    when (typeMap[mainAttr]) {
                        "int" ->  bound.setInt("main", mainFilter.toInt())
                        "date" -> {
                            val date = mainFilter.split("-")
                            bound.setDate("main", LocalDate.fromYearMonthDay(date[0].toInt(), date[1].toInt(), date[2].toInt()))
                        }
                        "text" -> bound.setString("main", mainFilter)
                        "timestamp" -> TODO("type not supported yet")
                        else -> TODO("type not supported yet")
                    }
                }

                endLog()

                endLog = marker.startLogDuration("CQL statement execution")
                val res = session.execute(bound)
                endLog()


                endLog = marker.startLogDuration("fetch features")
                val features = res.map { row ->

                    val attrMap = attributes.map { attr ->
                        when (typeMap[attr]) {
                            "int" -> attr to Value.IntValue(row.getInt(attr).toLong())
                            "double" -> attr to Value.DoubleValue(row.getDouble(attr))
                            "date" -> attr to Value.StringValue(row.getDate(attr).toString())
                            "text" -> attr to Value.StringValue(row.getString(attr).toString())
                            "timestamp" -> TODO("type not supported yet")
                            else -> TODO("type not supported yet")
                        }
                    }.toMap()

                    projector.projectFeature(
                            Feature(
                            geometry = Geometry.fromWKT(row.getString("geometry"))!!,
                            properties = attrMap,
                            id = "0"
                            )
                    )

                }
                endLog()
                endLog = marker.startLogDuration("prepare features for encoding")
                val geojson = GeoJSON(features = features)

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

            get("/heatmap/{z}/{x}/{y}") {
                val z = call.parameters["z"]?.toInt()?:-1
                val x = call.parameters["x"]?.toInt()?:-1
                val y = call.parameters["y"]?.toInt()?:-1

                val box = projector.tileBBox(z, x, y)

                val poly = Geometry.Polygon(coordinates = listOf(listOf(
                        listOf(box[0], box[1]),
                        listOf(box[2], box[1]),
                        listOf(box[2], box[3]),
                        listOf(box[0], box[3]),
                        listOf(box[0], box[1])
                )))
                val tileFeature = Feature(geometry = poly)
                projector.calcBbox(tileFeature)
                val bbox = tileFeature.bbox

                val clipper = Clipper()

                val n = when (z) {
                    in (1..5) -> 24
                    in (6..9) -> 24
                    else -> 16
                }
                val xDelta = (bbox[2] - bbox[0])/n
                val yDelta = (bbox[3] - bbox[1])/n
                val boxes = (0 until n).map { i ->
                    (0 until n).map { j ->
                        val aa = clipper.clip(tileFeature, 1.0, bbox[0] + (i * xDelta), bbox[0] + ((i+1) * xDelta), bbox[1] + (j * yDelta), bbox[1] + ((j+1) * yDelta) )
//                        println(aa)
                        aa
                    }
//                    when (i) {
//                        0 -> clipper.clip(tileFeature, 1.0, bbox[0], bbox[0] + (xDelta/2), bbox[1], bbox[1] + (yDelta/2), false)
//                        1 -> clipper.clip(tileFeature, 1.0, bbox[0] + (xDelta/2), bbox[2], bbox[1], bbox[1] + (yDelta/2), false)
//                        2 -> clipper.clip(tileFeature, 1.0, bbox[0], bbox[0] + (xDelta/2), bbox[1] + (yDelta/2), bbox[3], false)
//                        3 -> clipper.clip(tileFeature, 1.0, bbox[0] + (xDelta/2), bbox[2], bbox[1] + (yDelta/2), bbox[3], false)
//                        else -> null
//                    }
                }.fold(listOf<Feature?>()) { r, l ->
                    r + l
                }

                val fs = boxes.map { f ->
                    val centroid = f!!.geometry.toJTS().centroid
                    val tileNumber1 = projector.getTileNumber(centroid.y, centroid.x, 5)
                    val hash1 = GeoHashUtils.encode(
                            projector.tileToLat(tileNumber1.third, 5),
                            projector.tileToLon(tileNumber1.second, 5)
                    )

                    val tileNumber2 = projector.getTileNumber(centroid.y, centroid.x, 10)
                    val hash2 = GeoHashUtils.encode(
                            projector.tileToLat(tileNumber2.third, 10),
                            projector.tileToLon(tileNumber2.second, 10)
                    )

                    val jsonQuery = """
                    {
                        filter: {
                         type: "geo_shape",
                         field: "geometry",
                         operation: "intersects",
                         shape: {
                            type: "wkt",
                            value: "${f.geometry.toWKT()}"
                         }
                        }
                    }
                """.trimIndent()

                    val bound = qHeatmap.bind()
                            .setString("json", jsonQuery)
                            .setString("hash", hash1)
                            .setString("hash2", hash2)
                    val res = session.execute(bound)
                    val count = Value.IntValue(res.elementAt(0).getLong("count"))

                    projector.projectFeature(Feature(geometry = f.geometry, properties = mapOf("count" to count)))
                }

                val tile = projector.transformTile(Tile(GeoJSON(features = fs), (1 shl z), x, y))

                val encoder = Encoder()

                val encoded = encoder.encode(tile.geojson.features, baseLayer)

                call.respondBytes(encoded.toByteArray())
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

    private fun initCassandra(
            clusterBuilder: Cluster.Builder,
            strategy: String,
            replication: Int,
            keyspace: String,
            geoIndex: String,
            table: String,
            datacenter: String,
            partitionKeys: List<String>,
            primaryKeys: List<String>,
            attributes: List<String>
    ): Boolean {
        val cluster = clusterBuilder.build()
        val session = cluster.connect()
        if (strategy == "SimpleStrategy") {
            session.execute("CREATE  KEYSPACE IF NOT EXISTS $keyspace " +
                    "WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : $replication };")
        } else {
            session.execute("CREATE  KEYSPACE IF NOT EXISTS $keyspace " +
                    "WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', '$datacenter' : $replication};")
        }

        val tableQuery = """
            |CREATE TABLE IF NOT EXISTS $keyspace.$table
            | (geohash_data text, geohash_heatmap text, ${if (attributes.isNotEmpty()) attributes.joinToString(", ", "", ", ") else ""} geometry text,
            | PRIMARY KEY ((${partitionKeys.joinToString(", ")}) ${if (primaryKeys.isNotEmpty()) primaryKeys.joinToString(",", ", ") else ""}));
        """.trimMargin().replace("\n".toRegex(), "")

        val indexQuery = """
            |CREATE CUSTOM INDEX IF NOT EXISTS $geoIndex ON
            | $keyspace.$table (geometry) USING 'com.stratio.cassandra.lucene.Index'
            | WITH OPTIONS = {
            |   'refresh_seconds': '60',
            |   'partitioner': '{type: "token", partitions: 4}',
            |    'schema': '{
            |       fields: {
            |           geometry: {
            |               type: "geo_shape",
            |               max_levels: 3,
            |               transformations: [{type: "bbox"}]
            |           }
            |        }
            |     }'
            |};
        """.trimMargin().replace("\n".toRegex(), "")

        session.execute("USE $keyspace;")
        session.execute(tableQuery)
        session.execute(indexQuery)
        session.close()
        cluster.close()
        return true
    }