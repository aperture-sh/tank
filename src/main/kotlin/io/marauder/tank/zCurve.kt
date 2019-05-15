package io.marauder.tank

object ZcurveUtils {
    private val B = listOf(0x55555555, 0x33333333, 0x0F0F0F0F, 0x00FF00FF)
    private val S = listOf(1, 2, 4, 8)

    // http://stackoverflow.com/questions/4909263/how-to-efficiently-de-interleave-bits-inverse-morton
    fun deinterleave(z: Int) : Int
    {
        var x = z
        x = x and 0x55555555
        x = (x or (x shr 1)) and 0x33333333
        x = (x or (x shr 2)) and 0x0F0F0F0F
        x = (x or (x shr 4)) and 0x00FF00FF
        x = (x or (x shr 8)) and 0x0000FFFF
        return x
    }

    // http://graphics.stanford.edu/~seander/bithacks.html#InterleaveBMN
    fun interleave(x1: Int, y1: Int): Int {
        var x = (x1 or (x1 shl S[3])) and B[3]
        x = (x or (x shl S[2])) and B[2]
        x = (x or (x shl S[1])) and B[1]
        x = (x or (x shl S[0])) and B[0]
        var y = (y1 or (y1 shl S[3])) and B[3]
        y = (y or (y shl S[2])) and B[2]
        y = (y or (y shl S[1])) and B[1]
        y = (y or (y shl S[0])) and B[0]
        val z = x or (y shl 1)
        return z
    }
}
