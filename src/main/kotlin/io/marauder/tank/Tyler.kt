package io.marauder.tank

import com.datastax.driver.core.LocalDate
import com.datastax.driver.core.Session
import com.datastax.driver.core.exceptions.OperationTimedOutException
import com.datastax.driver.core.exceptions.QueryExecutionException
import io.marauder.charged.Projector
import io.marauder.charged.models.Feature
import io.marauder.charged.models.GeoJSON
import io.marauder.charged.models.Value
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.serialization.ImplicitReflectionSerializer
import net.spy.memcached.MemcachedClient
import org.slf4j.LoggerFactory
import java.lang.ClassCastException
import java.util.*

@ImplicitReflectionSerializer
class Tyler(
        private val session: Session,
        dbTable: String = "features",
        addTimeStamp: Boolean = true,
        private val attrFields: List<String>,
        private val hashLevel: Int,
        private val exhauster: Exhauster?,
        memcachedEnabled: Boolean = false,
        mcc : MemcachedClient?,
        chacheRegionCount: Int,
        cacheRegionThreshold: Int,
        cacheZoomLevelEnd:Int,
        cacheBoundingThreshold:Int
) {

    private val attributes = attrFields.map { it.split(" ").first() }
    private val q = session.prepare("""
        INSERT INTO $dbTable (hash, ${if (attributes.isNotEmpty()) attributes.joinToString(", ", "", ",") else "" } geometry)
        VALUES (:hash, ${ if (attributes.isNotEmpty()) attributes.joinToString(", ", "", ", ") { if (addTimeStamp && it == "timestamp") "unixTimestampOf(now())" else ":$it" } else "" } :geometry)
    """.trimIndent())

    private val projector = Projector()
    private val regionManager = RegionManager(chacheRegionCount, cacheRegionThreshold, memcachedEnabled, cacheZoomLevelEnd, mcc)
    private val boundingManager = BoundingManager(cacheBoundingThreshold, memcachedEnabled, cacheZoomLevelEnd, mcc)

    private var delay = 0L

    @ImplicitReflectionSerializer
    suspend fun import(input: GeoJSON) {

        log.info("#${input.features.size} features importing starts")
        input.features.forEachIndexed { i, f ->
            import(f)
            if (i % 1000 == 0) log.info("#$i features stored to DB")
        }

        log.info("#${input.features.size} features importing finished")
    }

    suspend fun import(f: Feature, uuid: UUID? = null) : UUID? {
        var endLog = marker.startLogDuration("prepare geometry")
        val actualUUID = uuid ?: UUID.randomUUID()

        retry@do {
            try {
                val bound = q.bind()
                attrFields.forEach { attr ->
                    val (name, type) = attr.split(" ")


                    if (name !in listOf("timestamp", "uid")) {
                        @Suppress("IMPLICIT_CAST_TO_ANY")
                        val propertyValue = if (f.properties[name] != null) {

                            when (type) {
                                "int" -> (f.properties[name] as Value.IntValue).value.toInt()
                                "double" -> {
                                    try {
                                        (f.properties[name] as Value.DoubleValue).value
                                    } catch (e: ClassCastException) {
                                        when (f.properties[name]) {
                                            is Value.IntValue -> (f.properties[name] as Value.IntValue).value.toDouble()
                                            is Value.StringValue -> (f.properties[name] as Value.StringValue).value.toDouble()
                                            else -> TODO("type not supported yet")
                                        }
                                    }
                                }
                                "text" -> {
                                    try {
                                        (f.properties[name] as Value.StringValue).value
                                    } catch (e: ClassCastException) {
                                        when (f.properties[name]) {
                                            is Value.IntValue -> (f.properties[name] as Value.IntValue).value.toString()
                                            is Value.DoubleValue -> (f.properties[name] as Value.DoubleValue).value.toString()
                                            else -> TODO("type not supported yet")
                                        }
                                    }
                                }
                                "date" -> {
                                    val date = (f.properties["img_date"] as Value.StringValue).value.split('-')
                                    LocalDate.fromYearMonthDay(date[0].toInt(), date[1].toInt(), date[2].toInt())
                                }
                                else -> TODO("type not supported yet")
                            }

                        } else {
                            when (type) {
                                "int" -> 0
                                "double" -> 0.0
                                "text" -> ""
                                "date" -> {
                                    LocalDate.fromYearMonthDay(1970, 1, 1)
                                }
                                else -> TODO("type not supported yet")
                            }
                        }
                        when (type) {
                            "int" -> bound.setInt(name, propertyValue as Int)
                            "double" -> bound.setDouble(name, propertyValue as Double)
                            "text" -> bound.setString(name, propertyValue as String)
                            "date" -> bound.setDate(name, propertyValue as LocalDate)
                            else -> TODO("type not supported yet")
                        }
                    }
                }

                val centroid = f.geometry.toJTS().centroid
                val tileNumber = projector.getTileNumber(centroid.y, centroid.x, hashLevel)


                val hash = ZcurveUtils.interleave(tileNumber.second, tileNumber.third)

                bound.setString("geometry", f.geometry.toWKT())
                bound.setInt("hash", hash)
                bound.setUUID("uid", actualUUID)

                endLog()

                regionManager.add(f)
                //boundingManager.add(f)

                endLog = marker.startLogDuration("store geometry to database")
                delay(delay)
                session.execute(bound)
                if (delay > 1000) {
                    delay -= 1000
                } else {
                    delay = 0
                }
                endLog()
                break@retry
            } catch (e: ClassCastException) {
                if (exhauster != null) {
                    GlobalScope.launch {
                        exhauster.pushFeature(actualUUID, f)
                    }
                } else {
                    log.warn("Feature skipped due property type collision (cause ${e.message}).")
                }
                return null
            } catch (e: NumberFormatException) {
                if (exhauster != null) {
                    GlobalScope.launch {
                        exhauster.pushFeature(actualUUID, f)
                    }
                } else {
                    log.warn("Feature skipped due property type collision (cause ${e.message}).")
                }
                return null
            } catch (e: QueryExecutionException) {
                log.warn("Increasing query execution delay due high DB usage (now at $delay ms, cause ${e.message})")
                delay += delay + 1000
            } catch (e: OperationTimedOutException) {
                log.warn("Increasing query execution delay due high DB usage (now at $delay ms, cause ${e.message})")
                delay += delay + 1000
            }

        } while (true)
        return actualUUID
    }

    fun closeCaching() {
        regionManager.flush()
        boundingManager.flush()
    }

    companion object {
        private val log = LoggerFactory.getLogger(Tyler::class.java)
        private val marker = Benchmark(log)
    }

}
