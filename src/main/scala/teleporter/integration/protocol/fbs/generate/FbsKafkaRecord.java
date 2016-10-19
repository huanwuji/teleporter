// automatically generated, do not modify

package teleporter.integration.protocol.fbs.generate;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class FbsKafkaRecord extends Table {
  public static FbsKafkaRecord getRootAsFbsKafkaRecord(ByteBuffer _bb) { return getRootAsFbsKafkaRecord(_bb, new FbsKafkaRecord()); }
  public static FbsKafkaRecord getRootAsFbsKafkaRecord(ByteBuffer _bb, FbsKafkaRecord obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public FbsKafkaRecord __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public byte tId(int j) { int o = __offset(4); return o != 0 ? bb.get(__vector(o) + j * 1) : 0; }
  public int tIdLength() { int o = __offset(4); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer tIdAsByteBuffer() { return __vector_as_bytebuffer(4, 1); }
  public String topic() { int o = __offset(6); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer topicAsByteBuffer() { return __vector_as_bytebuffer(6, 1); }
  public byte key(int j) { int o = __offset(8); return o != 0 ? bb.get(__vector(o) + j * 1) : 0; }
  public int keyLength() { int o = __offset(8); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer keyAsByteBuffer() { return __vector_as_bytebuffer(8, 1); }
  public int partition() { int o = __offset(10); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public byte data(int j) { int o = __offset(12); return o != 0 ? bb.get(__vector(o) + j * 1) : 0; }
  public int dataLength() { int o = __offset(12); return o != 0 ? __vector_len(o) : 0; }
  public ByteBuffer dataAsByteBuffer() { return __vector_as_bytebuffer(12, 1); }

  public static int createFbsKafkaRecord(FlatBufferBuilder builder,
      int tIdOffset,
      int topicOffset,
      int keyOffset,
      int partition,
      int dataOffset) {
    builder.startObject(5);
    FbsKafkaRecord.addData(builder, dataOffset);
    FbsKafkaRecord.addPartition(builder, partition);
    FbsKafkaRecord.addKey(builder, keyOffset);
    FbsKafkaRecord.addTopic(builder, topicOffset);
    FbsKafkaRecord.addTId(builder, tIdOffset);
    return FbsKafkaRecord.endFbsKafkaRecord(builder);
  }

  public static void startFbsKafkaRecord(FlatBufferBuilder builder) { builder.startObject(5); }
  public static void addTId(FlatBufferBuilder builder, int tIdOffset) { builder.addOffset(0, tIdOffset, 0); }
  public static int createTIdVector(FlatBufferBuilder builder, byte[] data) { builder.startVector(1, data.length, 1); for (int i = data.length - 1; i >= 0; i--) builder.addByte(data[i]); return builder.endVector(); }
  public static void startTIdVector(FlatBufferBuilder builder, int numElems) { builder.startVector(1, numElems, 1); }
  public static void addTopic(FlatBufferBuilder builder, int topicOffset) { builder.addOffset(1, topicOffset, 0); }
  public static void addKey(FlatBufferBuilder builder, int keyOffset) { builder.addOffset(2, keyOffset, 0); }
  public static int createKeyVector(FlatBufferBuilder builder, byte[] data) { builder.startVector(1, data.length, 1); for (int i = data.length - 1; i >= 0; i--) builder.addByte(data[i]); return builder.endVector(); }
  public static void startKeyVector(FlatBufferBuilder builder, int numElems) { builder.startVector(1, numElems, 1); }
  public static void addPartition(FlatBufferBuilder builder, int partition) { builder.addInt(3, partition, 0); }
  public static void addData(FlatBufferBuilder builder, int dataOffset) { builder.addOffset(4, dataOffset, 0); }
  public static int createDataVector(FlatBufferBuilder builder, byte[] data) { builder.startVector(1, data.length, 1); for (int i = data.length - 1; i >= 0; i--) builder.addByte(data[i]); return builder.endVector(); }
  public static void startDataVector(FlatBufferBuilder builder, int numElems) { builder.startVector(1, numElems, 1); }
  public static int endFbsKafkaRecord(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
};

