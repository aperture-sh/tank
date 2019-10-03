package io.marauder.tank

import io.marauder.charged.Projector
import io.marauder.charged.models.Feature
import io.marauder.charged.models.Geometry
import kotlin.math.pow
import kotlin.math.sqrt


class Region {
    var bbox =  mutableListOf<Double>()
    private val projector = Projector()
    private var halfDiameter = 0.0
    var featureCount = 0
    var distanceToLastStep = 0

    fun add(f: Feature) = add(f.bbox)

    fun safeAdd(bb: Region?) {
        if(bb != null){
            featureCount += bb.featureCount
            add(bb.bbox)
        }
    }

    fun distanceTo(f: Feature) : Double = distanceTo(f.bbox)

    fun distanceTo(bb: MutableList<Double>) : Double {
        var dist = bBoxToGeometry(bb).toJTS().distance(bBoxToGeometry(bbox).toJTS())
        dist -= getMinimalRadiusOfBoundingCircle(bb)
        dist -= halfDiameter
        return dist
    }

    fun getGeometry() = bBoxToGeometry(bbox)

    private fun add(bb: MutableList<Double>) {
        if(bbox.size == 0) {
            bbox = bb
        }
        else {
            projector.calcBbox(bBoxToCoordinatList(bb), bbox)
        }

        halfDiameter = getMinimalRadiusOfBoundingCircle(bbox)
        featureCount++
    }

    private fun bBoxToCoordinatList(bounding: List<Double>) = listOf(
            listOf(bounding[0], bounding[1]),
            listOf(bounding[0], bounding[3]),
            listOf(bounding[2], bounding[3]),
            listOf(bounding[2], bounding[1]),
            listOf(bounding[0], bounding[1]))

    private fun bBoxToGeometry(bounding: List<Double>) = Geometry.Polygon(coordinates = listOf(bBoxToCoordinatList(bounding)))

    private fun getMinimalRadiusOfBoundingCircle(bb: MutableList<Double>) = 0.5 * sqrt((bb[0] - bb[2]).pow(2.0) + (bb[1] - bb[3]).pow(2.0))
}