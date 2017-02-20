// automatically generated by the FlatBuffers compiler, do not modify

package teleporter.integration.cluster.rpc.fbs;

import java.nio.*;
import java.lang.*;
import java.util.*;

import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class KVS extends Table {
    public static KVS getRootAsKVS(ByteBuffer _bb) {
        return getRootAsKVS(_bb, new KVS());
    }

    public static KVS getRootAsKVS(ByteBuffer _bb, KVS obj) {
        _bb.order(ByteOrder.LITTLE_ENDIAN);
        return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb));
    }

    public KVS __init(int _i, ByteBuffer _bb) {
        bb_pos = _i;
        bb = _bb;
        return this;
    }

    public KV kvs(int j) {
        return kvs(new KV(), j);
    }

    public KV kvs(KV obj, int j) {
        int o = __offset(4);
        return o != 0 ? obj.__init(__indirect(__vector(o) + j * 4), bb) : null;
    }

    public int kvsLength() {
        int o = __offset(4);
        return o != 0 ? __vector_len(o) : 0;
    }

    public static int createKVS(FlatBufferBuilder builder,
                                int kvsOffset) {
        builder.startObject(1);
        KVS.addKvs(builder, kvsOffset);
        return KVS.endKVS(builder);
    }

    public static void startKVS(FlatBufferBuilder builder) {
        builder.startObject(1);
    }

    public static void addKvs(FlatBufferBuilder builder, int kvsOffset) {
        builder.addOffset(0, kvsOffset, 0);
    }

    public static int createKvsVector(FlatBufferBuilder builder, int[] data) {
        builder.startVector(4, data.length, 4);
        for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]);
        return builder.endVector();
    }

    public static void startKvsVector(FlatBufferBuilder builder, int numElems) {
        builder.startVector(4, numElems, 4);
    }

    public static int endKVS(FlatBufferBuilder builder) {
        int o = builder.endObject();
        return o;
    }
}
