// automatically generated, do not modify

package teleporter.integration.protocol.fbs.generate;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class JdbcParam extends Table {
  public static JdbcParam getRootAsJdbcParam(ByteBuffer _bb) { return getRootAsJdbcParam(_bb, new JdbcParam()); }
  public static JdbcParam getRootAsJdbcParam(ByteBuffer _bb, JdbcParam obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public JdbcParam __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public int type() { int o = __offset(4); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public byte value(int j) { int o = __offset(6); return o != 0 ? bb.get(__vector(o) + j * 1) : 0; }
  public int valueLength() { int o = __offset(6); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer valueAsByteBuffer() { return __vector_as_bytebuffer(6, 1); }

  public static int createJdbcParam(FlatBufferBuilder builder,
      int type,
      int valueOffset) {
    builder.startObject(2);
    JdbcParam.addValue(builder, valueOffset);
    JdbcParam.addType(builder, type);
    return JdbcParam.endJdbcParam(builder);
  }

  public static void startJdbcParam(FlatBufferBuilder builder) { builder.startObject(2); }
  public static void addType(FlatBufferBuilder builder, int type) { builder.addInt(0, type, 0); }
  public static void addValue(FlatBufferBuilder builder, int valueOffset) { builder.addOffset(1, valueOffset, 0); }
  public static int createValueVector(FlatBufferBuilder builder, byte[] data) { builder.startVector(1, data.length, 1); for (int i = data.length - 1; i >= 0; i--) builder.addByte(data[i]); return builder.endVector(); }
  public static void startValueVector(FlatBufferBuilder builder, int numElems) { builder.startVector(1, numElems, 1); }
  public static int endJdbcParam(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
};

