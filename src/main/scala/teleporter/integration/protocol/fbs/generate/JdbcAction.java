// automatically generated, do not modify

package teleporter.integration.protocol.fbs.generate;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class JdbcAction extends Table {
  public static JdbcAction getRootAsJdbcAction(ByteBuffer _bb) { return getRootAsJdbcAction(_bb, new JdbcAction()); }
  public static JdbcAction getRootAsJdbcAction(ByteBuffer _bb, JdbcAction obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public JdbcAction __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public byte type() { int o = __offset(4); return o != 0 ? bb.get(o + bb_pos) : 0; }
  public JdbcStatement statements(int j) { return statements(new JdbcStatement(), j); }
  public JdbcStatement statements(JdbcStatement obj, int j) { int o = __offset(6); return o != 0 ? obj.__init(__indirect(__vector(o) + j * 4), bb) : null; }
  public int statementsLength() { int o = __offset(6); return o != 0 ? __vector_len(o) : 0; }

  public static int createJdbcAction(FlatBufferBuilder builder,
      byte type,
      int statementsOffset) {
    builder.startObject(2);
    JdbcAction.addStatements(builder, statementsOffset);
    JdbcAction.addType(builder, type);
    return JdbcAction.endJdbcAction(builder);
  }

  public static void startJdbcAction(FlatBufferBuilder builder) { builder.startObject(2); }
  public static void addType(FlatBufferBuilder builder, byte type) { builder.addByte(0, type, 0); }
  public static void addStatements(FlatBufferBuilder builder, int statementsOffset) { builder.addOffset(1, statementsOffset, 0); }
  public static int createStatementsVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startStatementsVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static int endJdbcAction(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
};

