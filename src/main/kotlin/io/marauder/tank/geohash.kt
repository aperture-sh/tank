package io.marauder.tank

import java.util.HashMap

/**
 * Utilities for encoding and decoding geohashes. Based on
 * [http://en.wikipedia.org/wiki/Geohash.
](http://en.wikipedia.org/wiki/Geohash) */
object GeoHashUtils {

    private val BASE_32 = charArrayOf('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z')

    private val DECODE_MAP = HashMap<Char, Int>()

    private const val PRECISION = 12
    private val BITS = intArrayOf(16, 8, 4, 2, 1)

    init {
        for (i in BASE_32.indices) {
            DECODE_MAP[Character.valueOf(BASE_32[i])] = Integer.valueOf(i)
        }
    }

    /**
     * Encodes the given latitude and longitude into a geohash
     *
     * @param latitude Latitude to encode
     * @param longitude Longitude to encode
     * @return Geohash encoding of the longitude and latitude
     */
    fun encode(latitude: Double, longitude: Double): String {
        val latInterval = doubleArrayOf(-90.0, 90.0)
        val lngInterval = doubleArrayOf(-180.0, 180.0)

        val geohash = StringBuilder()
        var isEven = true

        var bit = 0
        var ch = 0

        while (geohash.length < PRECISION) {
            var mid = 0.0
            if (isEven) {
                mid = (lngInterval[0] + lngInterval[1]) / 2.0
                if (longitude > mid) {
                    ch = ch or BITS[bit]
                    lngInterval[0] = mid
                } else {
                    lngInterval[1] = mid
                }
            } else {
                mid = (latInterval[0] + latInterval[1]) / 2.0
                if (latitude > mid) {
                    ch = ch or BITS[bit]
                    latInterval[0] = mid
                } else {
                    latInterval[1] = mid
                }
            }

            isEven = !isEven

            if (bit < 4) {
                bit++
            } else {
                geohash.append(BASE_32[ch])
                bit = 0
                ch = 0
            }
        }

        return geohash.toString()
    }

    /**
     * Decodes the given geohash into a latitude and longitude
     *
     * @param geohash Geohash to deocde
     * @return Array with the latitude at index 0, and longitude at index 1
     */
    fun decode(geohash: String): DoubleArray {
        val latInterval = doubleArrayOf(-90.0, 90.0)
        val lngInterval = doubleArrayOf(-180.0, 180.0)

        var isEven = true

        val latitude: Double
        val longitude: Double
        for (i in 0 until geohash.length) {
            val cd = DECODE_MAP[Character.valueOf(
                    geohash[i])]!!.toInt()

            for (mask in BITS) {
                if (isEven) {
                    if (cd and mask != 0) {
                        lngInterval[0] = (lngInterval[0] + lngInterval[1]) / 2.0
                    } else {
                        lngInterval[1] = (lngInterval[0] + lngInterval[1]) / 2.0
                    }
                } else {
                    if (cd and mask != 0) {
                        latInterval[0] = (latInterval[0] + latInterval[1]) / 2.0
                    } else {
                        latInterval[1] = (latInterval[0] + latInterval[1]) / 2.0
                    }
                }
                isEven = !isEven
            }

        }
        latitude = (latInterval[0] + latInterval[1]) / 2.0
        longitude = (lngInterval[0] + lngInterval[1]) / 2.0

        return doubleArrayOf(latitude, longitude)
    }
}