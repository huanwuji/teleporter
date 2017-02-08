// automatically generated by the FlatBuffers compiler, do not modify

package teleporter.integration.cluster.rpc.fbs;

import java.nio.*;
import java.lang.*;
import java.util.*;

import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class LogTailRequest extends Table {
    public static LogTailRequest getRootAsLogTailRequest(ByteBuffer _bb) {
        return getRootAsLogTailRequest(_bb, new LogTailRequest());
    }

    public static LogTailRequest getRootAsLogTailRequest(ByteBuffer _bb, LogTailRequest obj) {
        _bb.order(ByteOrder.LITTLE_ENDIAN);
        return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb));
    }

    public LogTailRequest __init(int _i, ByteBuffer _bb) {
        bb_pos = _i;
        bb = _bb;
        return this;
    }

    public int request() {
        int o = __offset(4);
        return o != 0 ? bb.getInt(o + bb_pos) : 0;
    }

    public String cmd() {
        int o = __offset(6);
        return o != 0 ? __string(o + bb_pos) : null;
    }

    public ByteBuffer cmdAsByteBuffer() {
        return __vector_as_bytebuffer(6, 1);
    }

    public static int createLogTailRequest(FlatBufferBuilder builder,
                                           int request,
                                           int cmdOffset) {
        builder.startObject(2);
        LogTailRequest.addCmd(builder, cmdOffset);
        LogTailRequest.addRequest(builder, request);
        return LogTailRequest.endLogTailRequest(builder);
    }

    public static void startLogTailRequest(FlatBufferBuilder builder) {
        builder.startObject(2);
    }

    public static void addRequest(FlatBufferBuilder builder, int request) {
        builder.addInt(0, request, 0);
    }

    public static void addCmd(FlatBufferBuilder builder, int cmdOffset) {
        builder.addOffset(1, cmdOffset, 0);
    }

    public static int endLogTailRequest(FlatBufferBuilder builder) {
        int o = builder.endObject();
        return o;
    }
}

