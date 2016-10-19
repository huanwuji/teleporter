// automatically generated, do not modify

package teleporter.integration.protocol.fbs.generate;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class JdbcMessages extends Table {
  public static JdbcMessages getRootAsJdbcMessages(ByteBuffer _bb) { return getRootAsJdbcMessages(_bb, new JdbcMessages()); }
  public static JdbcMessages getRootAsJdbcMessages(ByteBuffer _bb, JdbcMessages obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public JdbcMessages __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public JdbcMessage messages(int j) { return messages(new JdbcMessage(), j); }
  public JdbcMessage messages(JdbcMessage obj, int j) { int o = __offset(4); return o != 0 ? obj.__init(__indirect(__vector(o) + j * 4), bb) : null; }
  public int messagesLength() { int o = __offset(4); return o != 0 ? __vector_len(o) : 0; }

  public static int createJdbcMessages(FlatBufferBuilder builder,
      int messagesOffset) {
    builder.startObject(1);
    JdbcMessages.addMessages(builder, messagesOffset);
    return JdbcMessages.endJdbcMessages(builder);
  }

  public static void startJdbcMessages(FlatBufferBuilder builder) { builder.startObject(1); }
  public static void addMessages(FlatBufferBuilder builder, int messagesOffset) { builder.addOffset(0, messagesOffset, 0); }
  public static int createMessagesVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startMessagesVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static int endJdbcMessages(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
};

