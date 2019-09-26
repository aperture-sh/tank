package io.marauder.tank

import io.marauder.charged.Projector
import io.marauder.charged.models.Feature
import io.marauder.charged.models.Geometry
import net.spy.memcached.MemcachedClient
import org.slf4j.Logger
import java.util.ArrayList


class RegionManager(
        private val cacheRegionCount: Int,
        private val cacheRegionThreshold: Int,
        private val memcachedEnabled: Boolean = false,
        private val cacheZoomLevelEnd: Int,
        private val mcc : MemcachedClient?,
        private val log: Logger
) {
    private val regions = mutableListOf<Region>()
    private val projector = Projector()

    // Distance-Werte
    private var biggestDistance = 0.0
    private var shortestDistance = Double.MAX_VALUE
    private var shortPair:Pair<Region, Region>? = null

    // Debugging
    private var removeTileRequests = 0
    private var removeRequests = 0
    private var mergedRegions = 0
    private var featuresImported = 0
    private var destroyedRegions = 0

    fun add(f: Feature) {
        if(!memcachedEnabled) return


        featuresImported++
        projector.calcBbox(f)
        // Falls nicht alle Regionen gefüllt sind
        if(regions.size < cacheRegionCount) {
            val newRegion = addToRegions(f)
            recalculateDistantces(newRegion)
        }
        // Falls es nur eine Region geben soll
        else if(cacheRegionCount == 1) {
            addInRegion(0, f)
        }
        else{
            val index = lowestDistance(f)
            if(index == -1) {
                // Merge die nähsten Regionen und erstelle eine neu aus f
                if(shortPair != null) {
                    mergedRegions++
                    shortPair?.first?.safeAdd(shortPair?.second)
                    regions.remove(shortPair?.second)
                    addToRegions(f)

                    // Recalculiere alle Distanze-Werte
                    biggestDistance = 0.0
                    shortestDistance = Double.MAX_VALUE
                    regions.forEach {r->recalculateDistantces(r)}
                }
            }
            else {
                // Füge f in region mit Index index ein.
                addInRegion(index, f)

                // Füge jeder Regionen, die in diesem Schritt nicht bearbeitet wurde einen Schritt im Zähler hinzu
                // und überprüfe, dass die Gesamtzahl unter dem Schwellwert liegt. Wurde der Schwellwert überschritten
                // Lösche die Region
                val remove = mutableListOf<Region>()
                for ((i, r) in regions.withIndex()) {
                    if(index == i)
                            r.distanceToLastStep = 0
                    else {
                        r.distanceToLastStep++
                        if(r.distanceToLastStep > cacheRegionThreshold) {
                            remove.add(r)
                            invalCacheCV(regions[i].getGeometry())
                        }
                    }
                }
                remove.forEach { r ->
                    regions.remove(r)
                }
                regions.forEach {r->recalculateDistantces(r)}
                destroyedRegions += remove.size
            }
        }
    }

    // Sucht nach dem Index in der Region-Liste, in die das Feature f eingeflochten werden soll
    // Gibt den Index zurück, oder -1, falls die Distanz des Features zu allen Regions größer ist
    // als der größte Abstand zwischen den Regions
    private fun lowestDistance(f: Feature): Int {
        var lowDist = Double.MAX_VALUE
        var index = -1

        for ((i, r) in regions.withIndex()) {
            val dist = r.distanceTo(f)
            if(lowDist > dist){
                lowDist = dist
                index = i
            }
        }

        if(lowDist > biggestDistance)
            return -1

        return index
    }

    private fun addToRegions(f: Feature): Region{
        val newRegion = Region(log)
        regions.add(newRegion)
        newRegion.add(f)
        return newRegion
    }

    private fun addInRegion(index:Int, f:Feature) {
        regions[index].add(f)

        if(regions[index].featureCount < cacheRegionThreshold)
            recalculateDistantces(regions[index])
        else {
            invalCacheCV(regions[index].getGeometry())
            regions.removeAt(index)
            biggestDistance = 0.0
            shortestDistance = Double.MAX_VALUE
            regions.forEach {r->recalculateDistantces(r)}
        }
    }

    private fun recalculateDistantces(region:Region) {
        regions.forEach {r ->
            if(region != r) {
                val dist = r.distanceTo(region.bbox)
                if(biggestDistance < dist)
                    biggestDistance = dist

                if(shortestDistance > dist){
                    shortestDistance = dist
                    shortPair = Pair(region, r)
                }
            }
        }
    }

    fun flush() {
        if(!memcachedEnabled) return

        regions.forEach {r ->
            invalCacheCV(r.getGeometry())
        }
        regions.clear()
        log.info("Tile-Requests = $removeTileRequests  Remove-Requests = $removeRequests  Merge-Requests = $mergedRegions  Features-Imported: $featuresImported Destroyed Regions: $destroyedRegions")
        removeRequests = 0
        removeTileRequests = 0
        mergedRegions = 0
        featuresImported = 0
    }


    /**
     * Covering
     */
    private fun invalCacheCV(geo : Geometry) {
        removeRequests++
        val tileLookUp = ArrayList<Tile>()

        tileLookUp.add(Tile(0,0,0))

        while(tileLookUp.isNotEmpty()) {
            if(tileLookUp[0].z <= cacheZoomLevelEnd) {
                if(tileLookUp[0].getGeometry().toJTS().coveredBy(geo.toJTS())) {
                    invalCacheAllChildren(tileLookUp[0])
                }

                else if(tileLookUp[0].getGeometry().toJTS().intersects(geo.toJTS())) {
                    tileLookUp.addAll(tileLookUp[0].getChildren())
                    removeTile(tileLookUp[0])
                }
            }
            tileLookUp.removeAt(0)
        }
    }

    private fun invalCacheAllChildren(t : Tile) {
        val queue = ArrayList<Tile>()

        queue.add(t)

        while(queue.isNotEmpty()) {
            if(queue[0].z < cacheZoomLevelEnd) {
                queue.addAll(queue[0].getChildren())
            }
            removeTile(queue[0])
            queue.removeAt(0)
        }
    }

    private fun removeTile(t : Tile) {
        if(memcachedEnabled) {
            mcc?.delete("heatmap/" + t.z + "/" + t.x + "/" + t.y)
            mcc?.delete("tile/" + t.z + "/" + t.x + "/" + t.y)
            removeTileRequests++
        }
    }
}