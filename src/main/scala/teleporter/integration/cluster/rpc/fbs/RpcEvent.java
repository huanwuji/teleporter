// automatically generated by the FlatBuffers compiler, do not modify

package teleporter.integration.cluster.rpc.fbs;

import java.nio.*;
import java.lang.*;
import java.util.*;

import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class RpcEvent extends Table {
    public static RpcEvent getRootAsRpcEvent(ByteBuffer _bb) {
        return getRootAsRpcEvent(_bb, new RpcEvent());
    }

    public static RpcEvent getRootAsRpcEvent(ByteBuffer _bb, RpcEvent obj) {
        _bb.order(ByteOrder.LITTLE_ENDIAN);
        return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb));
    }

    public RpcEvent __init(int _i, ByteBuffer _bb) {
        bb_pos = _i;
        bb = _bb;
        return this;
    }

    public long seqNr() {
        int o = __offset(4);
        return o != 0 ? bb.getLong(o + bb_pos) : 0;
    }

    public byte eventType() {
        int o = __offset(6);
        return o != 0 ? bb.get(o + bb_pos) : 0;
    }

    public byte role() {
        int o = __offset(8);
        return o != 0 ? bb.get(o + bb_pos) : 0;
    }

    public byte status() {
        int o = __offset(10);
        return o != 0 ? bb.get(o + bb_pos) : 0;
    }

    public byte body(int j) {
        int o = __offset(12);
        return o != 0 ? bb.get(__vector(o) + j * 1) : 0;
    }

    public int bodyLength() {
        int o = __offset(12);
        return o != 0 ? __vector_len(o) : 0;
    }

    public ByteBuffer bodyAsByteBuffer() {
        return __vector_as_bytebuffer(12, 1);
    }

    public static int createRpcEvent(FlatBufferBuilder builder,
                                     long seqNr,
                                     byte eventType,
                                     byte role,
                                     byte status,
                                     int bodyOffset) {
        builder.startObject(5);
        RpcEvent.addSeqNr(builder, seqNr);
        RpcEvent.addBody(builder, bodyOffset);
        RpcEvent.addStatus(builder, status);
        RpcEvent.addRole(builder, role);
        RpcEvent.addEventType(builder, eventType);
        return RpcEvent.endRpcEvent(builder);
    }

    public static void startRpcEvent(FlatBufferBuilder builder) {
        builder.startObject(5);
    }

    public static void addSeqNr(FlatBufferBuilder builder, long seqNr) {
        builder.addLong(0, seqNr, 0);
    }

    public static void addEventType(FlatBufferBuilder builder, byte eventType) {
        builder.addByte(1, eventType, 0);
    }

    public static void addRole(FlatBufferBuilder builder, byte role) {
        builder.addByte(2, role, 0);
    }

    public static void addStatus(FlatBufferBuilder builder, byte status) {
        builder.addByte(3, status, 0);
    }

    public static void addBody(FlatBufferBuilder builder, int bodyOffset) {
        builder.addOffset(4, bodyOffset, 0);
    }

    public static int createBodyVector(FlatBufferBuilder builder, byte[] data) {
        builder.startVector(1, data.length, 1);
        for (int i = data.length - 1; i >= 0; i--) builder.addByte(data[i]);
        return builder.endVector();
    }

    public static void startBodyVector(FlatBufferBuilder builder, int numElems) {
        builder.startVector(1, numElems, 1);
    }

    public static int endRpcEvent(FlatBufferBuilder builder) {
        int o = builder.endObject();
        return o;
    }
}
