// automatically generated by the FlatBuffers compiler, do not modify

package teleporter.integration.cluster.rpc.fbs;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class HealthResponse extends Table {
  public static HealthResponse getRootAsHealthResponse(ByteBuffer _bb) { return getRootAsHealthResponse(_bb, new HealthResponse()); }
  public static HealthResponse getRootAsHealthResponse(ByteBuffer _bb, HealthResponse obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public HealthResponse __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public float totalMemory() { int o = __offset(4); return o != 0 ? bb.getFloat(o + bb_pos) : 0.0f; }
  public float freeMemory() { int o = __offset(6); return o != 0 ? bb.getFloat(o + bb_pos) : 0.0f; }

  public static int createHealthResponse(FlatBufferBuilder builder,
      float totalMemory,
      float freeMemory) {
    builder.startObject(2);
    HealthResponse.addFreeMemory(builder, freeMemory);
    HealthResponse.addTotalMemory(builder, totalMemory);
    return HealthResponse.endHealthResponse(builder);
  }

  public static void startHealthResponse(FlatBufferBuilder builder) { builder.startObject(2); }
  public static void addTotalMemory(FlatBufferBuilder builder, float totalMemory) { builder.addFloat(0, totalMemory, 0.0f); }
  public static void addFreeMemory(FlatBufferBuilder builder, float freeMemory) { builder.addFloat(1, freeMemory, 0.0f); }
  public static int endHealthResponse(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
}

