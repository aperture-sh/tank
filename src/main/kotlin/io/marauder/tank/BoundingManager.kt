package io.marauder.tank

import io.marauder.charged.models.Feature
import io.marauder.charged.models.Geometry
import net.spy.memcached.MemcachedClient
import org.slf4j.Logger
import java.util.ArrayList

class BoundingManager (private val cacheBoundingThreshold: Int,
                       private val memcachedEnabled: Boolean = false,
                       private val cacheZoomLevelEnd: Int,
                       private val mcc : MemcachedClient?,
                       private val log: Logger
){
    private val tilingSet = arrayListOf<Tile>()



    // Debugging
    private var removeTileRequests = 0
    private var removeRequests = 0
    private var featuresImported = 0
    private var calcTiles = 0


    fun add(f: Feature) {
        if(!memcachedEnabled) return

        featuresImported++

        invalCacheCV(f.geometry)

        if(tilingSet.size >= cacheBoundingThreshold) {
            clearTilingSet()
        }
    }


    private fun clearTilingSet() {
        removeRequests++
        tilingSet.forEach{t->
            removeTile(t)
        }
        tilingSet.clear()
    }


    fun flush() {
        if(!memcachedEnabled) return
        clearTilingSet()

        tilingSet.clear()

        log.info("Tile-Requests = $removeTileRequests  Remove-Requests = $removeRequests  Features-Imported: $featuresImported calculated Tiles= $calcTiles" )
        removeRequests = 0
        removeTileRequests = 0
        featuresImported = 0
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
                    if(!tilingSet.contains(tileLookUp[0]))
                        tilingSet.add(tileLookUp[0])
                    calcTiles++
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
            if(!tilingSet.contains(queue[0]))
                tilingSet.add(queue[0])
            queue.removeAt(0)
            calcTiles++
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