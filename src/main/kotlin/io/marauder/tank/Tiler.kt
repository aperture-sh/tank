package io.marauder.tank.tiling

import com.datastax.driver.core.Session
import io.marauder.supercharged.Clipper
import io.marauder.supercharged.Encoder
import io.marauder.supercharged.Intersector
import io.marauder.supercharged.Projector
import io.marauder.supercharged.models.GeoJSON
import io.marauder.supercharged.models.Tile
import io.marauder.tank.Benchmark
import kotlinx.coroutines.*
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.json.JSON
import kotlinx.serialization.stringify
import org.slf4j.LoggerFactory
import java.nio.Buffer
import java.nio.ByteBuffer
import java.util.UUID
import kotlin.math.pow

class Tiler(
        private val session: Session,
        private val minZoom: Int = 2,
        private val maxZoom: Int = 15,
        private val extend: Int = 4096,
        private val buffer: Int = 64) {

    private val projector = Projector(extend)
    private val intersector = Intersector()
    private val clipper = Clipper()
    private val q = session.prepare("INSERT INTO features (z,x,y,id,geometry) VALUES (?, ?, ?, ?, ?)")


    @ImplicitReflectionSerializer
    fun tiler(input: GeoJSON) {

            println("calculating bounding box: ${input.features.size} features")
            projector.calcBbox(input)
            println("start split")

            //TODO: wrap geometries at 180 degree
            //wrap -> left + 1 (offset), right - 1 (offset)
            /*val buffer: Double = BUFFER.toDouble() / EXTENT
        val left = clip(input, 1.0, -1 -buffer, buffer, 0)
        val right = clip(input, 1.0, 1 -buffer, 2+buffer, 0)
        val center = clip(input, 1.0, -buffer, 1+buffer, 0)*/

//        val merged = FeatureCollection(features = left.features + right.features + center.features)

            (minZoom..maxZoom).forEach { zoomLvL ->
                println("\rzoom level: $zoomLvL")
                    traverseZoom(input, zoomLvL)

            }
            println("\rfinished split: ${input.features.size} features")
    }

    @ImplicitReflectionSerializer
    private fun traverseZoom(f: GeoJSON, z: Int) {
        (0..(2.0.pow(z.toDouble()).toInt())).forEach { x ->
            val boundCheck = intersector.fcOutOfBounds(f, (1 shl z).toDouble(), (x).toDouble(), (1 + x).toDouble(), 0)

            if (boundCheck == 1) return@forEach
            if (boundCheck == 0)
                (0..(2.0.pow(z.toDouble()).toInt())).forEach { y ->
                    split(f, z, x, y)
                }
        }
    }

    @ImplicitReflectionSerializer
    private fun split(f: GeoJSON, z: Int, x: Int, y: Int) {
        val z2 = 1 shl (if (z == 0) 0 else z)

        val k1 = 0.5 * buffer / extend
        val k3 = 1 + k1


        var endLog = marker.startLogDuration("clipping - featureCount={} type={} z={} x={} y={}",
                f.features.size, f.type.name, z, x, y)
        val clipped = clipper.clip(f, z2.toDouble(), x - k1, x + k3, y - k1, y + k3)
        endLog()

        val encoder = Encoder()

        if (clipped.features.isNotEmpty()) {

            endLog = marker.startLogDuration("calculating bbox")
            projector.calcBbox(clipped)
            endLog()
            println("\rencode: $z/$x/$y::${clipped.features.size}")

            endLog = marker.startLogDuration("transforming tiles")
            val trans = projector.transformTile(Tile(clipped, 1 shl z, x, y))
            endLog()

            trans.geojson.features.forEach {
                endLog = marker.startLogDuration("encoding geometry - {}", it.geometry)
                val g = encoder.encodeGeometry(it.geometry)
                endLog()
//                println(it.geometry)
//                println(g)
                val buf = ByteBuffer.wrap(JSON.plain.stringify(g).toByteArray())
                val bound = q.bind()
                        .setInt(0, z)
                        .setInt(1, x)
                        .setInt(2, y)
                        .setString(3, UUID.randomUUID().toString())
                        .setBytes(4, buf)

                endLog = marker.startLogDuration("store geometry to database")
                session.execute(bound)
                endLog()
            }

        }
    }

    companion object {
        private val marker = Benchmark(LoggerFactory.getLogger(Tiler::class.java))
    }

}
