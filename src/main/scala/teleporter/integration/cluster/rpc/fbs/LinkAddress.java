// automatically generated by the FlatBuffers compiler, do not modify

package teleporter.integration.cluster.rpc.fbs;

import java.nio.*;
import java.lang.*;
import java.util.*;

import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class LinkAddress extends Table {
    public static LinkAddress getRootAsLinkAddress(ByteBuffer _bb) {
        return getRootAsLinkAddress(_bb, new LinkAddress());
    }

    public static LinkAddress getRootAsLinkAddress(ByteBuffer _bb, LinkAddress obj) {
        _bb.order(ByteOrder.LITTLE_ENDIAN);
        return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb));
    }

    public LinkAddress __init(int _i, ByteBuffer _bb) {
        bb_pos = _i;
        bb = _bb;
        return this;
    }

    public String address() {
        int o = __offset(4);
        return o != 0 ? __string(o + bb_pos) : null;
    }

    public ByteBuffer addressAsByteBuffer() {
        return __vector_as_bytebuffer(4, 1);
    }

    public String instance() {
        int o = __offset(6);
        return o != 0 ? __string(o + bb_pos) : null;
    }

    public ByteBuffer instanceAsByteBuffer() {
        return __vector_as_bytebuffer(6, 1);
    }

    public String keys(int j) {
        int o = __offset(8);
        return o != 0 ? __string(__vector(o) + j * 4) : null;
    }

    public int keysLength() {
        int o = __offset(8);
        return o != 0 ? __vector_len(o) : 0;
    }

    public long timestamp() {
        int o = __offset(10);
        return o != 0 ? bb.getLong(o + bb_pos) : 0;
    }

    public static int createLinkAddress(FlatBufferBuilder builder,
                                        int addressOffset,
                                        int instanceOffset,
                                        int keysOffset,
                                        long timestamp) {
        builder.startObject(4);
        LinkAddress.addTimestamp(builder, timestamp);
        LinkAddress.addKeys(builder, keysOffset);
        LinkAddress.addInstance(builder, instanceOffset);
        LinkAddress.addAddress(builder, addressOffset);
        return LinkAddress.endLinkAddress(builder);
    }

    public static void startLinkAddress(FlatBufferBuilder builder) {
        builder.startObject(4);
    }

    public static void addAddress(FlatBufferBuilder builder, int addressOffset) {
        builder.addOffset(0, addressOffset, 0);
    }

    public static void addInstance(FlatBufferBuilder builder, int instanceOffset) {
        builder.addOffset(1, instanceOffset, 0);
    }

    public static void addKeys(FlatBufferBuilder builder, int keysOffset) {
        builder.addOffset(2, keysOffset, 0);
    }

    public static int createKeysVector(FlatBufferBuilder builder, int[] data) {
        builder.startVector(4, data.length, 4);
        for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]);
        return builder.endVector();
    }

    public static void startKeysVector(FlatBufferBuilder builder, int numElems) {
        builder.startVector(4, numElems, 4);
    }

    public static void addTimestamp(FlatBufferBuilder builder, long timestamp) {
        builder.addLong(3, timestamp, 0);
    }

    public static int endLinkAddress(FlatBufferBuilder builder) {
        int o = builder.endObject();
        return o;
    }
}

