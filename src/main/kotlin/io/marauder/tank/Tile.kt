package io.marauder.tank

import io.marauder.charged.Projector
import io.marauder.charged.models.Geometry
import java.util.ArrayList


class Tile (val x: Int, val y: Int, val z: Int){

    private val projector = Projector()
    val box = projector.tileBBox(z, x, y)

    val poly = Geometry.Polygon(coordinates = listOf(listOf(
            listOf(box[0], box[1]),
            listOf(box[2], box[1]),
            listOf(box[2], box[3]),
            listOf(box[0], box[3]),
            listOf(box[0], box[1])
    )))



    fun getChildren(): ArrayList<Tile> {
        val response = ArrayList<Tile>()

        response.add(Tile(2 * x, 2 * y, z + 1))
        response.add(Tile(2 * x + 1, 2 * y, z + 1))
        response.add(Tile(2 * x, 2 * y + 1, z + 1))
        response.add(Tile(2 * x + 1, 2 * y + 1, z + 1))

        return response
    }

    fun getGeometry(): Geometry {
        return poly
    }

    fun getIntersection(geo: Geometry): Double {
        val intersect = geo.toJTS().intersection(poly.toJTS())
        return intersect.area / poly.toJTS().area
    }

    override fun toString():String {
        return "Tile($x,$y, $z)"
    }

    override fun equals(other: Any?): Boolean =
        if (other is Tile) {
            x == other.x && y == other.y && z == other.z
        } else {
            false
        }


}