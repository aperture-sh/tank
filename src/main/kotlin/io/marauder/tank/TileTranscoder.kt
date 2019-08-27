package io.marauder.tank

import net.spy.memcached.transcoders.Transcoder;
import net.spy.memcached.CachedData;

class TileTranscoder : Transcoder<ByteArray> {

    private val flags = 238885206

    override fun decode(d: CachedData): ByteArray {
        assert(d.getFlags() === flags) { "expected " + flags + " got " + d.getFlags() }
        return d.getData();
    }

    override fun encode(o: ByteArray): CachedData {
        return CachedData(flags, o, getMaxSize())
    }

    override fun getMaxSize(): Int {
        return CachedData.MAX_SIZE
    }

    override fun asyncDecode(d: CachedData): Boolean {
        return false
    }

}