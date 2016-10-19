// automatically generated, do not modify

package teleporter.integration.protocol.fbs.generate;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class JdbcMessage extends Table {
  public static JdbcMessage getRootAsJdbcMessage(ByteBuffer _bb) { return getRootAsJdbcMessage(_bb, new JdbcMessage()); }
  public static JdbcMessage getRootAsJdbcMessage(ByteBuffer _bb, JdbcMessage obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public JdbcMessage __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public byte tid(int j) { int o = __offset(4); return o != 0 ? bb.get(__vector(o) + j * 1) : 0; }
  public int tidLength() { int o = __offset(4); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer tidAsByteBuffer() { return __vector_as_bytebuffer(4, 1); }
  public JdbcAction actions(int j) { return actions(new JdbcAction(), j); }
  public JdbcAction actions(JdbcAction obj, int j) { int o = __offset(6); return o != 0 ? obj.__init(__indirect(__vector(o) + j * 4), bb) : null; }
  public int actionsLength() { int o = __offset(6); return o != 0 ? __vector_len(o) : 0; }

  public static int createJdbcMessage(FlatBufferBuilder builder,
      int tidOffset,
      int actionsOffset) {
    builder.startObject(2);
    JdbcMessage.addActions(builder, actionsOffset);
    JdbcMessage.addTid(builder, tidOffset);
    return JdbcMessage.endJdbcMessage(builder);
  }

  public static void startJdbcMessage(FlatBufferBuilder builder) { builder.startObject(2); }
  public static void addTid(FlatBufferBuilder builder, int tidOffset) { builder.addOffset(0, tidOffset, 0); }
  public static int createTidVector(FlatBufferBuilder builder, byte[] data) { builder.startVector(1, data.length, 1); for (int i = data.length - 1; i >= 0; i--) builder.addByte(data[i]); return builder.endVector(); }
  public static void startTidVector(FlatBufferBuilder builder, int numElems) { builder.startVector(1, numElems, 1); }
  public static void addActions(FlatBufferBuilder builder, int actionsOffset) { builder.addOffset(1, actionsOffset, 0); }
  public static int createActionsVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startActionsVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static int endJdbcMessage(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
};

