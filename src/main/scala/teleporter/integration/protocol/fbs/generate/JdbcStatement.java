// automatically generated, do not modify

package teleporter.integration.protocol.fbs.generate;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class JdbcStatement extends Table {
  public static JdbcStatement getRootAsJdbcStatement(ByteBuffer _bb) { return getRootAsJdbcStatement(_bb, new JdbcStatement()); }
  public static JdbcStatement getRootAsJdbcStatement(ByteBuffer _bb, JdbcStatement obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public JdbcStatement __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public String sql() { int o = __offset(4); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer sqlAsByteBuffer() { return __vector_as_bytebuffer(4, 1); }
  public JdbcParam params(int j) { return params(new JdbcParam(), j); }
  public JdbcParam params(JdbcParam obj, int j) { int o = __offset(6); return o != 0 ? obj.__init(__indirect(__vector(o) + j * 4), bb) : null; }
  public int paramsLength() { int o = __offset(6); return o != 0 ? __vector_len(o) : 0; }

  public static int createJdbcStatement(FlatBufferBuilder builder,
      int sqlOffset,
      int paramsOffset) {
    builder.startObject(2);
    JdbcStatement.addParams(builder, paramsOffset);
    JdbcStatement.addSql(builder, sqlOffset);
    return JdbcStatement.endJdbcStatement(builder);
  }

  public static void startJdbcStatement(FlatBufferBuilder builder) { builder.startObject(2); }
  public static void addSql(FlatBufferBuilder builder, int sqlOffset) { builder.addOffset(0, sqlOffset, 0); }
  public static void addParams(FlatBufferBuilder builder, int paramsOffset) { builder.addOffset(1, paramsOffset, 0); }
  public static int createParamsVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startParamsVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static int endJdbcStatement(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
};

