// automatically generated by the FlatBuffers compiler, do not modify

package teleporter.integration.cluster.rpc.fbs;

import java.nio.*;
import java.lang.*;
import java.util.*;

import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class BrokerState extends Table {
    public static BrokerState getRootAsBrokerState(ByteBuffer _bb) {
        return getRootAsBrokerState(_bb, new BrokerState());
    }

    public static BrokerState getRootAsBrokerState(ByteBuffer _bb, BrokerState obj) {
        _bb.order(ByteOrder.LITTLE_ENDIAN);
        return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb));
    }

    public BrokerState __init(int _i, ByteBuffer _bb) {
        bb_pos = _i;
        bb = _bb;
        return this;
    }

    public String broker() {
        int o = __offset(4);
        return o != 0 ? __string(o + bb_pos) : null;
    }

    public ByteBuffer brokerAsByteBuffer() {
        return __vector_as_bytebuffer(4, 1);
    }

    public String task() {
        int o = __offset(6);
        return o != 0 ? __string(o + bb_pos) : null;
    }

    public ByteBuffer taskAsByteBuffer() {
        return __vector_as_bytebuffer(6, 1);
    }

    public long timestamp() {
        int o = __offset(8);
        return o != 0 ? bb.getLong(o + bb_pos) : 0;
    }

    public static int createBrokerState(FlatBufferBuilder builder,
                                        int brokerOffset,
                                        int taskOffset,
                                        long timestamp) {
        builder.startObject(3);
        BrokerState.addTimestamp(builder, timestamp);
        BrokerState.addTask(builder, taskOffset);
        BrokerState.addBroker(builder, brokerOffset);
        return BrokerState.endBrokerState(builder);
    }

    public static void startBrokerState(FlatBufferBuilder builder) {
        builder.startObject(3);
    }

    public static void addBroker(FlatBufferBuilder builder, int brokerOffset) {
        builder.addOffset(0, brokerOffset, 0);
    }

    public static void addTask(FlatBufferBuilder builder, int taskOffset) {
        builder.addOffset(1, taskOffset, 0);
    }

    public static void addTimestamp(FlatBufferBuilder builder, long timestamp) {
        builder.addLong(2, timestamp, 0);
    }

    public static int endBrokerState(FlatBufferBuilder builder) {
        int o = builder.endObject();
        return o;
    }
}
