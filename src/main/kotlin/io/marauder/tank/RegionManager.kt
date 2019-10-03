package io.marauder.tank

import io.marauder.charged.Projector
import io.marauder.charged.models.Feature
import io.marauder.charged.models.Geometry
import net.spy.memcached.MemcachedClient
import java.util.ArrayList


class RegionManager(
        private val cacheRegionCount: Int,
        private val cacheRegionThreshold: Int,
        private val memcachedEnabled: Boolean = false,
        private val cacheZoomLevelEnd: Int,
        private val mcc : MemcachedClient?
) {
    private val regions = mutableListOf<Region>()
    private val projector = Projector()

    // Distance-Values
    private var biggestDistance = 0.0
    private var shortestDistance = Double.MAX_VALUE
    private var shortPair:Pair<Region, Region>? = null

    fun add(f: Feature) {
        if(!memcachedEnabled) return

        projector.calcBbox(f)
        if(regions.size < cacheRegionCount) {
            val newRegion = addToRegions(f)
            recalculateDistantces(newRegion)
        }

        else if(cacheRegionCount == 1) {
            addInRegion(0, f)
        }

        else{
            val index = lowestDistance(f)
            if(index == -1) {
                // Merge the closest regions and create a new one from f
                if(shortPair != null) {
                    shortPair?.first?.safeAdd(shortPair?.second)
                    regions.remove(shortPair?.second)
                    addToRegions(f)

                    // Recalculate all distance-values
                    biggestDistance = 0.0
                    shortestDistance = Double.MAX_VALUE
                    regions.forEach {r->recalculateDistantces(r)}
                }
            }
            else {
                // Add f to region at index
                addInRegion(index, f)

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
            }
        }
    }

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
        val newRegion = Region()
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
    }

    /**
     * Covering
     */
    private fun invalCacheCV(geo : Geometry) {
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
        }
    }
}