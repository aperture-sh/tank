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
import com.datastax.driver.core.exceptions.OperationTimedOutException
import com.datastax.driver.core.exceptions.QueryExecutionException
import com.google.gson.Gson
import io.ktor.util.KtorExperimentalAPI
import kotlinx.serialization.stringify
import java.io.File
import java.lang.Exception
import java.util.UUID

import net.spy.memcached.MemcachedClient
import java.net.ConnectException
import java.net.InetSocketAddress


fun main(args: Array<String>): Unit = io.ktor.server.jetty.EngineMain.main(args)


    @KtorExperimentalAPI
    @InternalAPI
    @ImplicitReflectionSerializer
    fun Application.module() {

        val marker = Benchmark(LoggerFactory.getLogger(this::class.java))

        val tmpDirectory = environment.config.propertyOrNull("ktor.application.tmp_dir")?.getString()
                ?: "./tmp"
        val prefix = environment.config.propertyOrNull("ktor.deployment.prefix")?.getString() ?: ""

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
        val hashLevel = environment.config.propertyOrNull("ktor.application.data.hash_level")?.getString()?.toInt() ?: 13

        val exhausterHost = environment.config.propertyOrNull("ktor.application.exhauster.host")?.getString() ?: "localhost"
        val exhausterPort = environment.config.propertyOrNull("ktor.application.exhauster.port")?.getString()?.toInt() ?: 8080
        val exhausterEnabled = environment.config.propertyOrNull("ktor.application.exhauster.enabled")?.getString()?.toBoolean() ?: false

        val zoomLevelStart = environment.config.propertyOrNull("ktor.application.cache_zoomlevel_start")?.getString()?.toInt() ?: 2
        val zoomLevelEnd = environment.config.propertyOrNull("ktor.application.cache_zoomlevel_end")?.getString()?.toInt() ?: 15
        val memcachedClientHost = environment.config.propertyOrNull("ktor.application.memcached_client_host")?.getString() ?: "127.0.0.1"
        val memcachedClientPort = environment.config.propertyOrNull("ktor.application.memcached_client_port")?.getString()?.toInt() ?: 11211
        var memcachedEnabled = environment.config.propertyOrNull("ktor.application.memcached_enabled")?.getString()?.toBoolean() ?: false

        val typeMap = attrFields.map { attr ->
            val (name, type) = attr.split(" ")
            name to type
        }.toMap()

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
                initCassandra(clusterBuilder, dbStrategy, dbReplFactor, dbKeyspace, dbGeoIndex, "uuid_index", dbTable, dbDatacenter, partitionKeys, primaryKeys, attrFields)
                isConnected = true
            } catch (e: RuntimeException) {
                e.printStackTrace()
                attempts--
                runBlocking {
                    delay(10_000)
                }
            }
        }

        var mcc:MemcachedClient? = null
        val tt = TileTranscoder()

        if(memcachedEnabled)
            try {
                mcc = MemcachedClient(InetSocketAddress(memcachedClientHost, memcachedClientPort))
            } catch (e: ConnectException) {
                memcachedEnabled = false
            }


        val cluster = clusterBuilder.build()
        val session = cluster.connect(dbKeyspace)
        val exhauster = if (exhausterEnabled) Exhauster(exhausterHost, exhausterPort) else null
        val tiler = Tyler(session, dbTable, addTimeStamp, attrFields, hashLevel, exhauster, memcachedEnabled, mcc)
        val fileWaitGroup = FileWaitGroup(tiler, tmpDirectory)
        val projector = Projector()

        val query = """
            | SELECT geometry${if (attributes.isNotEmpty()) attributes.joinToString(",", ", ", "") else "" }
            | FROM $dbTable
            | WHERE hash = :hash ${ if (mainAttr != "") "AND $mainAttr = :main" else "" };
            | """.trimMargin()

        val queryOne = """
                    SELECT * FROM $dbTable WHERE uid = :uuid;
                """.trimIndent()

        val queryDeleteOne = """
                    DELETE FROM $dbTable WHERE hash = :hash AND uid = :uuid;
                """.trimIndent()

        val deleteQuery = """
            | DELETE
            | FROM $dbTable
            | WHERE hash = :hash;
            | """.trimMargin()

        val countQuery = """
            | SELECT count(uid) AS count
            | FROM $dbTable
            | WHERE hash = :hash;
            | """.trimMargin()

        val q = session.prepare(query)
        val qOne = session.prepare(queryOne)
        val deleteOne = session.prepare(queryDeleteOne)
        val qDelete = session.prepare(deleteQuery)
        val qHeatmap = session.prepare(countQuery)

        GlobalScope.launch {
            fileWaitGroup.startRunner()
        }

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
            header("X-Engine", "Ktor")
            header("Access-Control-Allow-Origin", "*")
            header("Access-Control-Allow-Methods", "GET, POST, OPTIONS, DELETE")
            header("Access-Control-Allow-Headers", "X-Engine,DNT,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,Range")
            header("Access-Control-Expose-Headers", "Content-Length,Content-Range")
        }


        routing {
            route(prefix) {
                options("*") {
                    call.respondText("", contentType = ContentType.Text.Plain, status = HttpStatusCode.NoContent)
                }

                options("/") {
                    call.respondText("", contentType = ContentType.Text.Plain, status = HttpStatusCode.NoContent)
                }

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
                        try {
                            val stream = call.receiveStream()
                            stream.copyTo(importFile.outputStream())

                            if (call.parameters["geojson"] == "true") {
                                GlobalScope.launch {
                                    val input = JSON.plain.parse<GeoJSON>(importFile.readText())
                                    tiler.import(input)
                                    importFile.delete()
                                }
                            } else {
                                GlobalScope.launch {
                                    fileWaitGroup.startRunner()
                                }


                            }

                            call.respondText("{\"msg\": \"file accepted\", \"id\": \"$importId\"}", contentType = ContentType.Application.Json, status = HttpStatusCode.Accepted)
                        } catch (e: Exception) {
                            call.respondText("{\"msg\": \"${e.message}\"}", contentType = ContentType.Application.Json, status = HttpStatusCode.InternalServerError)
                            importFile.delete()
                        }
                    }
                }

                get("/{uuid}") {
                    val uuid = call.parameters["uuid"] ?: ""
                    val bound = qOne.bind().setUUID("uuid", UUID.fromString(uuid))
                    val featureRaw = session.execute(bound).first()
                    val properties: Map<String, Value> = typeMap.filter { it.key !in listOf("timestamp") }.map { attr ->
                        when (attr.value) {
                            "int" -> attr.key to Value.IntValue(featureRaw.getInt(attr.key).toLong())
                            "double" -> attr.key to Value.DoubleValue(featureRaw.getDouble(attr.key))
                            "date" -> attr.key to Value.StringValue(featureRaw.getDate(attr.key).toString())
                            "text" -> attr.key to Value.StringValue(featureRaw.getString(attr.key).toString())
                            "timestamp" -> TODO("type not supported yet")
                            "uuid" -> attr.key to Value.StringValue(featureRaw.getUUID(attr.key).toString())
                            else -> TODO("type not supported yet")
                        }
                    }.toMap()
                    val f = Feature(
                            id = uuid,
                            geometry = Geometry.fromWKT(featureRaw.getString("geometry"))!!,
                            properties = properties
                    )
                    call.respondText(status = HttpStatusCode.OK, text = JSON.indented.stringify(f), contentType = ContentType.Application.Json)
                }

                delete("/{uuid}") {
                    val uuid = call.parameters["uuid"] ?: ""
                    if (uuid != "") {
                        val boundGet = qOne.bind().setUUID("uuid", UUID.fromString(uuid))
                        val featureRaw = session.execute(boundGet).first()
                        val boundDelete = deleteOne.bind()
                                .setUUID("uuid", UUID.fromString(uuid))
                                .setInt("hash", featureRaw.getInt("hash"))
                        session.execute(boundDelete)
                        call.respondText(status = HttpStatusCode.OK, text = "{\"msg\": \"item deleted\", \"id\": \"$uuid\"}", contentType = ContentType.Application.Json)
                    } else {
                        call.respondText(status = HttpStatusCode.NotFound, text = "{\"msg\": \"item not found\", \"id\": \"$uuid\"}", contentType = ContentType.Application.Json)
                    }
                }

                get("/tile/{z}/{x}/{y}") {

                    var endLog = marker.startLogDuration("prepare query")
                    val z = call.parameters["z"]?.toInt() ?: -1
                    val x = call.parameters["x"]?.toInt() ?: -1
                    val y = call.parameters["y"]?.toInt() ?: -1


                    val cacheTile = if(memcachedEnabled && z in zoomLevelStart..zoomLevelEnd) mcc?.get("tile/$z/$x/$y", tt) else null

                    if(memcachedEnabled && cacheTile != null) {
                        call.respondBytes(cacheTile)
                    }
                    else {
                        // Lege eine Gson-Variable zur weiteren Verarbeitung fest
                        val gson = Gson()

                        val filters = gson.fromJson<Map<String, Any>>(call.parameters["filter"] ?: "{}", Map::class.java)

                        val mainFilter = (filters[mainAttr] ?: mainAttrDefault).toString()


                        val hashes = when {
                            z < hashLevel -> {
                                val delta = hashLevel - z
                                val xCurve1 = x shl delta
                                val yCurve1 = y shl delta
                                val xCurve2 = xCurve1 + Math.pow(2.toDouble(), delta.toDouble()).toInt() - 1
                                val yCurve2 = yCurve1 + Math.pow(2.toDouble(), delta.toDouble()).toInt() - 1
                                (ZcurveUtils.interleave(xCurve1, yCurve1)..ZcurveUtils.interleave(xCurve2, yCurve2)).toList()
                            }
                            z == hashLevel -> {
                                listOf(ZcurveUtils.interleave(x, y))
                            }
                            else -> {
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
                                val tileNumber = projector.getTileNumber(centroid.y, centroid.x, hashLevel)

                                listOf(ZcurveUtils.interleave(tileNumber.second, tileNumber.third))
                            }
                        }
                        endLog()

                        val features = hashes.flatMap { zCurve ->
                            val b = q.bind().setInt("hash", zCurve)
                            if (mainAttr !in listOf("", "*")) {
                                when (typeMap[mainAttr]) {
                                    "int" -> b.setInt("main", mainFilter.toInt())
                                    "date" -> {
                                        val date = mainFilter.split("-")
                                        b.setDate("main", LocalDate.fromYearMonthDay(date[0].toInt(), date[1].toInt(), date[2].toInt()))
                                    }
                                    "text" -> b.setString("main", mainFilter)
                                    "timestamp" -> TODO("type not supported yet")
                                    else -> TODO("type not supported yet")
                                }
                            }
                            endLog = marker.startLogDuration("CQL statement execution")
                            val res = session.execute(b)
                            endLog()
                            res.map { row ->

                                val attrMap = attributes.map { attr ->
                                    when (typeMap[attr]) {
                                        "int" -> attr to Value.IntValue(row.getInt(attr).toLong())
                                        "double" -> attr to Value.DoubleValue(row.getDouble(attr))
                                        "date" -> attr to Value.StringValue(row.getDate(attr).toString())
                                        "text" -> attr to Value.StringValue(row.getString(attr).toString())
                                        "timestamp" -> TODO("type not supported yet")
                                        "uuid" -> attr to Value.StringValue(row.getUUID(attr).toString())
                                        else -> TODO("type not supported yet")
                                    }
                                }.toMap()

                                endLog = marker.startLogDuration("Prepare features")
                                val projectedFeatures = projector.projectFeature(
                                        Feature(
                                                geometry = Geometry.fromWKT(row.getString("geometry"))!!,
                                                properties = attrMap,
                                                id = "0"
                                        )
                                )
                                endLog()
                                projectedFeatures
                            }
                        }

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

                        if(memcachedEnabled && z in zoomLevelStart..zoomLevelEnd)
                            mcc?.set("tile/$z/$x/$y", 10000, encoded.toByteArray(), tt)

                    }
                    endLog()
                }

                delete("/tile/{z}/{x}/{y}") {
                    val z = call.parameters["z"]?.toInt() ?: -1
                    val x = call.parameters["x"]?.toInt() ?: -1
                    val y = call.parameters["y"]?.toInt() ?: -1

                    val hashes = when {
                        z < hashLevel -> {
                            val delta = hashLevel - z
                            val xCurve1 = x shl delta
                            val yCurve1 = y shl delta
                            val xCurve2 = xCurve1 + Math.pow(2.toDouble(), delta.toDouble()).toInt() - 1
                            val yCurve2 = yCurve1 + Math.pow(2.toDouble(), delta.toDouble()).toInt() - 1
                            (ZcurveUtils.interleave(xCurve1, yCurve1)..ZcurveUtils.interleave(xCurve2, yCurve2)).toList()
                        }
                        z == hashLevel -> {
                            listOf(ZcurveUtils.interleave(x, y))
                        }
                        else -> {
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
                            val tileNumber = projector.getTileNumber(centroid.y, centroid.x, hashLevel)

                            listOf(ZcurveUtils.interleave(tileNumber.second, tileNumber.third))
                        }
                    }

                    hashes.forEach { zCurve ->
                        val b = qDelete.bind().setInt("hash", zCurve)
                        println(zCurve)
                        val res = session.execute(b)

                    }

                    call.respondText("ended")
                }

                get("/heatmap/{z}/{x}/{y}") {
                    val z = call.parameters["z"]?.toInt() ?: -1
                    val x = call.parameters["x"]?.toInt() ?: -1
                    val y = call.parameters["y"]?.toInt() ?: -1


                val cacheTile = if(memcachedEnabled && z in zoomLevelStart..zoomLevelEnd) mcc?.get("heatmap/$z/$x/$y", tt) else null

                if(cacheTile != null) {
                    call.respondBytes(cacheTile)
                }
                else {
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
                    val xDelta = (bbox[2] - bbox[0]) / n
                    val yDelta = (bbox[3] - bbox[1]) / n
                    val fs = (0 until n).map { i ->
                        (0 until n).map { j ->
                            clipper.clip(tileFeature, 1.0, bbox[0] + (i * xDelta), bbox[0] + ((i + 1) * xDelta), bbox[1] + (j * yDelta), bbox[1] + ((j + 1) * yDelta))
                        }
                    }.fold(listOf<Feature?>()) { r, l ->
                        r + l
                    }.map { f ->
                        val centroid = f!!.geometry.toJTS().centroid
                        val (_z, _x, _y) = projector.getTileNumber(centroid.y, centroid.x, hashLevel)

                        val count = when {
                            _z < hashLevel -> {
                                val delta = hashLevel - _z
                                val xCurve1 = _x shl delta
                                val yCurve1 = _y shl delta
                                val xCurve2 = xCurve1 + Math.pow(2.toDouble(), delta.toDouble()).toInt() - 1
                                val yCurve2 = yCurve1 + Math.pow(2.toDouble(), delta.toDouble()).toInt() - 1
                                (ZcurveUtils.interleave(xCurve1, yCurve1)..ZcurveUtils.interleave(xCurve2, yCurve2)).toList()
                            }
                            _z == hashLevel -> {
                                listOf(ZcurveUtils.interleave(_x, _y))
                            }
                            else -> {
                                val _box = projector.tileBBox(_z, _x, _y)

                                val _poly = Geometry.Polygon(coordinates = listOf(listOf(
                                        listOf(_box[0], _box[1]),
                                        listOf(_box[2], _box[1]),
                                        listOf(_box[2], _box[3]),
                                        listOf(_box[0], _box[3]),
                                        listOf(_box[0], _box[1])
                                )))

                                val _f = Feature(geometry = _poly)
                                val _centroid = _f.geometry.toJTS().centroid
                                val tileNumber = projector.getTileNumber(_centroid.y, _centroid.x, hashLevel)

                                listOf(ZcurveUtils.interleave(tileNumber.second, tileNumber.third))
                            }
                        }.fold(0L) { c, zCurve ->
                            val b = qHeatmap.bind().setInt("hash", zCurve)
                            val res = session.execute(b)
                            c + res.elementAt(0).getLong("count")
                        }

                        projector.projectFeature(Feature(geometry = f.geometry, properties = mapOf("count" to Value.IntValue(count))))
                    }.filter { (it.properties["count"] as Value.IntValue).value > 0 }

                    val tile = projector.transformTile(Tile(GeoJSON(features = fs), (1 shl z), x, y))

                    val encoder = Encoder()

                    val encoded = encoder.encode(tile.geojson.features, baseLayer)

                    call.respondBytes(encoded.toByteArray())

                    if(memcachedEnabled && z in zoomLevelStart..zoomLevelEnd)
                        mcc?.set("heatmap/$z/$x/$y", 10000, encoded.toByteArray(), tt)
                }
            }

                static("/static") {
                    resources("static")
                }

                install(StatusPages) {
                    exception<OutOfMemoryError> {
                        call.respondText(status = HttpStatusCode.InternalServerError, text = "{\"msg\": \"Out of memory: reduce file/bulk size\"}", contentType = ContentType.Application.Json)
                    }

                    exception<JsonParsingException> {
                        call.respondText(status = HttpStatusCode.InternalServerError, text = "{\"msg\": \"Json Parsing Issue: Check file format\"}", contentType = ContentType.Application.Json)
                    }

                    exception<QueryExecutionException> { cause ->
                        call.respondText(status = HttpStatusCode.InternalServerError, text = "{\"msg\": \"Database busy, try later\", \"cause\": \"${cause.message}\"}", contentType = ContentType.Application.Json)
                    }

                    exception<OperationTimedOutException> { cause ->
                        call.respondText(status = HttpStatusCode.InternalServerError, text = "{\"msg\": \"Database busy, try later\", \"cause\": \"${cause.message}\"}", contentType = ContentType.Application.Json)
                    }

                    exception<NotImplementedError> {
                        call.respondText(status = HttpStatusCode.InternalServerError, text = "{\"msg\": \"Not Implemented Yet, contact administrator\"}", contentType = ContentType.Application.Json)
                    }

                    exception<NoSuchElementException> {
                        call.respondText(status = HttpStatusCode.NotFound, text = "{\"msg\": \"item not found\"}", contentType = ContentType.Application.Json)
                    }

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
            uuidIndex: String,
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
            | (hash int, ${if (attributes.isNotEmpty()) attributes.joinToString(", ", "", ", ") else ""} geometry text,
            | PRIMARY KEY ((${partitionKeys.joinToString(", ")}) ${if (primaryKeys.isNotEmpty()) primaryKeys.joinToString(",", ", ") else ""}));
        """.trimMargin().replace("\n".toRegex(), "")

        val indexQueryLucene = """
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

        val indexQueryUUID = """
            |CREATE INDEX IF NOT EXISTS $uuidIndex ON
            | $keyspace.$table (uid);
        """.trimMargin().replace("\n".toRegex(), "")

        session.execute("USE $keyspace;")
        session.execute(tableQuery)
//        session.execute(indexQuery)
        session.execute(indexQueryUUID)
        session.close()
        cluster.close()
        return true
    }
