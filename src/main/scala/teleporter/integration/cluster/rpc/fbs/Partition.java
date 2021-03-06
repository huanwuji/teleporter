// automatically generated by the FlatBuffers compiler, do not modify

package teleporter.integration.cluster.rpc.fbs;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class Partition extends Table {
  public static Partition getRootAsPartition(ByteBuffer _bb) { return getRootAsPartition(_bb, new Partition()); }
  public static Partition getRootAsPartition(ByteBuffer _bb, Partition obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public Partition __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public String key() { int o = __offset(4); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer keyAsByteBuffer() { return __vector_as_bytebuffer(4, 1); }
  public String bootKeys(int j) { int o = __offset(6); return o != 0 ? __string(__vector(o) + j * 4) : null; }
  public int bootKeysLength() { int o = __offset(6); return o != 0 ? __vector_len(o) : 0; }

  public static int createPartition(FlatBufferBuilder builder,
      int keyOffset,
      int bootKeysOffset) {
    builder.startObject(2);
    Partition.addBootKeys(builder, bootKeysOffset);
    Partition.addKey(builder, keyOffset);
    return Partition.endPartition(builder);
  }

  public static void startPartition(FlatBufferBuilder builder) { builder.startObject(2); }
  public static void addKey(FlatBufferBuilder builder, int keyOffset) { builder.addOffset(0, keyOffset, 0); }
  public static void addBootKeys(FlatBufferBuilder builder, int bootKeysOffset) { builder.addOffset(1, bootKeysOffset, 0); }
  public static int createBootKeysVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startBootKeysVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static int endPartition(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
}

