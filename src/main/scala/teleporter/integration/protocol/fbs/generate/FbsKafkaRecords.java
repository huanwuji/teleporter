// automatically generated, do not modify

package teleporter.integration.protocol.fbs.generate;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class FbsKafkaRecords extends Table {
  public static FbsKafkaRecords getRootAsFbsKafkaRecords(ByteBuffer _bb) { return getRootAsFbsKafkaRecords(_bb, new FbsKafkaRecords()); }
  public static FbsKafkaRecords getRootAsFbsKafkaRecords(ByteBuffer _bb, FbsKafkaRecords obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public FbsKafkaRecords __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public FbsKafkaRecord records(int j) { return records(new FbsKafkaRecord(), j); }
  public FbsKafkaRecord records(FbsKafkaRecord obj, int j) { int o = __offset(4); return o != 0 ? obj.__init(__indirect(__vector(o) + j * 4), bb) : null; }
  public int recordsLength() { int o = __offset(4); return o != 0 ? __vector_len(o) : 0; }

  public static int createFbsKafkaRecords(FlatBufferBuilder builder,
      int recordsOffset) {
    builder.startObject(1);
    FbsKafkaRecords.addRecords(builder, recordsOffset);
    return FbsKafkaRecords.endFbsKafkaRecords(builder);
  }

  public static void startFbsKafkaRecords(FlatBufferBuilder builder) { builder.startObject(1); }
  public static void addRecords(FlatBufferBuilder builder, int recordsOffset) { builder.addOffset(0, recordsOffset, 0); }
  public static int createRecordsVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startRecordsVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static int endFbsKafkaRecords(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
};

