package io.marauder.tank

import io.marauder.charged.Projector
import io.marauder.charged.models.Feature
import io.marauder.charged.models.Geometry
import org.slf4j.Logger
import kotlin.math.pow
import kotlin.math.sqrt


class Region(
        private val log: Logger
) {

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

    private fun add(bb: MutableList<Double>) {
        if(bbox.size == 0) {
            bbox = bb
        }
        else {
            projector.calcBbox(bBoxToCoordinatList(bb), bbox)
        }
        halfDiameter = 0.5 * sqrt((bbox[0] - bbox[2]).pow(2.0) + (bbox[1] - bbox[3]).pow(2.0))
        featureCount++
    }

    private fun bBoxToCoordinatList(bounding: List<Double>) = listOf(
            listOf(bounding[0], bounding[1]),
            listOf(bounding[0], bounding[3]),
            listOf(bounding[2], bounding[3]),
            listOf(bounding[2], bounding[1]),
            listOf(bounding[0], bounding[1]))

    private fun bBoxToGeometry(bounding: List<Double>) = Geometry.Polygon(coordinates = listOf(bBoxToCoordinatList(bounding)))

    fun distanceTo(f: Feature) : Double = distanceTo(f.bbox)

    fun distanceTo(bb: MutableList<Double>) : Double {
        var dist = bBoxToGeometry(bb).toJTS().distance(bBoxToGeometry(bbox).toJTS())
        dist -= 0.5 * sqrt((bb[0] - bb[2]).pow(2.0) + (bb[1] - bb[3]).pow(2.0))
        dist -= halfDiameter
        return dist
    }

    fun getGeometry() = bBoxToGeometry(bbox)
}