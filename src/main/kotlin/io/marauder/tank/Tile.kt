package io.marauder.tank

import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.geom.Polygon
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.Geometry
import java.lang.Math.toDegrees
import java.util.ArrayList
import kotlin.math.atan
import kotlin.math.pow
import kotlin.math.sinh


class Tile (val x: Int, val y: Int, val z: Int){


    var poly = Polygon(GeometryFactory().createLinearRing(
            arrayOf(Coordinate(tile2lon(x, z), tile2lat(y, z)),
                    Coordinate(tile2lon(x + 1, z), tile2lat(y, z)),
                    Coordinate(tile2lon(x + 1, z), tile2lat(y + 1, z)),
                    Coordinate(tile2lon(x, z), tile2lat(y + 1, z)),
                    Coordinate(tile2lon(x, z), tile2lat(y, z))
            )),
            null,
            GeometryFactory())



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

    private fun tile2lon(x: Int, z: Int): Double {
        return x.toDouble() / 2.0.pow(z.toDouble()) * 360.0 - 180.0
    }

    private fun tile2lat(y: Int, z: Int): Double {
        val n = Math.PI - 2.0 * Math.PI * y.toDouble() / 2.0.pow(z.toDouble())
        return toDegrees(atan(sinh(n)))
    }

    fun getIntersection(geo: Geometry): Double {
        val intersect = geo.intersection(poly)
        return intersect.area / poly.area
    }

}