package io.marauder.tank

import io.marauder.charged.models.Feature
import io.marauder.charged.models.Geometry
import net.spy.memcached.MemcachedClient
import java.util.ArrayList

class BoundingManager (private val cacheBoundingThreshold: Int,
                       private val memcachedEnabled: Boolean = false,
                       private val cacheZoomLevelEnd: Int,
                       private val mcc : MemcachedClient?
){
    private val tilingSet = arrayListOf<Tile>()

    fun add(f: Feature) {
        if(!memcachedEnabled) return

        invalCacheCV(f.geometry)

        if(tilingSet.size >= cacheBoundingThreshold) {
            clearTilingSet()
        }
    }

    fun flush() {
        if(!memcachedEnabled) return

        clearTilingSet()

        tilingSet.clear()
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
                    safeAddToSet(tileLookUp[0])
                }
            }
            tileLookUp.removeAt(0)
        }
    }

    private fun clearTilingSet() {
        tilingSet.forEach{t->
            removeTile(t)
        }

        tilingSet.clear()
    }

    private fun invalCacheAllChildren(t : Tile) {
        val queue = ArrayList<Tile>()

        queue.add(t)

        while(queue.isNotEmpty()) {
            if(queue[0].z < cacheZoomLevelEnd) {
                queue.addAll(queue[0].getChildren())
            }
            safeAddToSet(queue[0])
            queue.removeAt(0)
        }
    }

    private fun safeAddToSet(t: Tile) {
        if(!tilingSet.contains(t))
            tilingSet.add(t)
    }


    private fun removeTile(t : Tile) {
        if(memcachedEnabled) {
            mcc?.delete("heatmap/" + t.z + "/" + t.x + "/" + t.y)
            mcc?.delete("tile/" + t.z + "/" + t.x + "/" + t.y)
        }
    }
}