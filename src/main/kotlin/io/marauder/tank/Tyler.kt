package io.marauder.tank

import com.datastax.driver.core.LocalDate
import com.datastax.driver.core.Session
import io.marauder.charged.Projector
import io.marauder.charged.models.GeoJSON
import io.marauder.charged.models.Value
import kotlinx.serialization.ImplicitReflectionSerializer
import org.slf4j.LoggerFactory
import java.lang.ClassCastException

class Tyler(
        private val session: Session,
        dbTable: String = "features",
        addTimeStamp: Boolean = true,
        private val attrFields: List<String>) {

    private val attributes = attrFields.map { it.split(" ").first() }
    private val q = session.prepare("""
        INSERT INTO $dbTable (hash, ${if (attributes.isNotEmpty()) attributes.joinToString(", ", "", ",") else "" } geometry)
        VALUES (:hash, ${ if (attributes.isNotEmpty()) attributes.map { if (addTimeStamp && it == "timestamp") "unixTimestampOf(now())" else ":$it" }.joinToString(", ", "", ", ") else "" } :geometry)
    """.trimIndent())

    private val projector = Projector()

    @ImplicitReflectionSerializer
    fun import(input: GeoJSON) {

        log.info("#${input.features.size} features importing starts")
        input.features.forEachIndexed { i, f ->
            var endLog = marker.startLogDuration("prepare geometry")

            try {
                val bound = q.bind()
                attrFields.forEach { attr ->
                    val (name, type) = attr.split(" ")


                    if (name != "timestamp") {
                        @Suppress("IMPLICIT_CAST_TO_ANY")
                        val propertyValue = if (f.properties[name] != null) {

                            when (type) {
                                "int" -> (f.properties[name] as Value.IntValue).value.toInt()
                                "double" -> (f.properties[name] as Value.DoubleValue).value
                                "text" -> (f.properties[name] as Value.StringValue).value
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
                val tileNumberData = projector.getTileNumber(centroid.y, centroid.x, 5)
                val tileNumberHeatmap = projector.getTileNumber(centroid.y, centroid.x, 13)
                val hashData = GeoHashUtils.encode(
                        projector.tileToLat(tileNumberData.third, tileNumberData.first),
                        projector.tileToLon(tileNumberData.second, tileNumberData.first))
                val hashHeatmap = GeoHashUtils.encode(
                        projector.tileToLat(tileNumberHeatmap.third, tileNumberHeatmap.first),
                        projector.tileToLon(tileNumberHeatmap.second, tileNumberHeatmap.first))

                val hash = ZcurveUtils.interleave(tileNumberHeatmap.second, tileNumberHeatmap.third)

                bound.setString("geometry", f.geometry.toWKT())
                bound.setInt("hash", hash)
//                bound.setString("geohash_heatmap", hashHeatmap)


                endLog()
                endLog = marker.startLogDuration("store geometry to database")
                session.execute(bound)
                endLog()
                if (i % 1000 == 0) log.info("#$i features stored to DB")
            } catch (e: ClassCastException) {
                log.warn("Feature skipped due property type collision.")
            }
        }

        log.info("#${input.features.size} features importing finished")
    }


    companion object {
        private val log = LoggerFactory.getLogger(Tyler::class.java)
        private val marker = Benchmark(log)
    }

}
