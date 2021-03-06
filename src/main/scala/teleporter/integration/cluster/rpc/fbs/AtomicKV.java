// automatically generated by the FlatBuffers compiler, do not modify

package teleporter.integration.cluster.rpc.fbs;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class AtomicKV extends Table {
  public static AtomicKV getRootAsAtomicKV(ByteBuffer _bb) { return getRootAsAtomicKV(_bb, new AtomicKV()); }
  public static AtomicKV getRootAsAtomicKV(ByteBuffer _bb, AtomicKV obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public AtomicKV __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public String key() { int o = __offset(4); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer keyAsByteBuffer() { return __vector_as_bytebuffer(4, 1); }
  public String expect() { int o = __offset(6); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer expectAsByteBuffer() { return __vector_as_bytebuffer(6, 1); }
  public String update() { int o = __offset(8); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer updateAsByteBuffer() { return __vector_as_bytebuffer(8, 1); }

  public static int createAtomicKV(FlatBufferBuilder builder,
      int keyOffset,
      int expectOffset,
      int updateOffset) {
    builder.startObject(3);
    AtomicKV.addUpdate(builder, updateOffset);
    AtomicKV.addExpect(builder, expectOffset);
    AtomicKV.addKey(builder, keyOffset);
    return AtomicKV.endAtomicKV(builder);
  }

  public static void startAtomicKV(FlatBufferBuilder builder) { builder.startObject(3); }
  public static void addKey(FlatBufferBuilder builder, int keyOffset) { builder.addOffset(0, keyOffset, 0); }
  public static void addExpect(FlatBufferBuilder builder, int expectOffset) { builder.addOffset(1, expectOffset, 0); }
  public static void addUpdate(FlatBufferBuilder builder, int updateOffset) { builder.addOffset(2, updateOffset, 0); }
  public static int endAtomicKV(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
}

