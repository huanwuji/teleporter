// automatically generated by the FlatBuffers compiler, do not modify

package teleporter.integration.cluster.rpc.fbs;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class TaskState extends Table {
  public static TaskState getRootAsTaskState(ByteBuffer _bb) { return getRootAsTaskState(_bb, new TaskState()); }
  public static TaskState getRootAsTaskState(ByteBuffer _bb, TaskState obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__init(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public TaskState __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; return this; }

  public String task() { int o = __offset(4); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer taskAsByteBuffer() { return __vector_as_bytebuffer(4, 1); }
  public String broker() { int o = __offset(6); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer brokerAsByteBuffer() { return __vector_as_bytebuffer(6, 1); }
  public long timestamp() { int o = __offset(8); return o != 0 ? bb.getLong(o + bb_pos) : 0; }

  public static int createTaskState(FlatBufferBuilder builder,
      int taskOffset,
      int brokerOffset,
      long timestamp) {
    builder.startObject(3);
    TaskState.addTimestamp(builder, timestamp);
    TaskState.addBroker(builder, brokerOffset);
    TaskState.addTask(builder, taskOffset);
    return TaskState.endTaskState(builder);
  }

  public static void startTaskState(FlatBufferBuilder builder) { builder.startObject(3); }
  public static void addTask(FlatBufferBuilder builder, int taskOffset) { builder.addOffset(0, taskOffset, 0); }
  public static void addBroker(FlatBufferBuilder builder, int brokerOffset) { builder.addOffset(1, brokerOffset, 0); }
  public static void addTimestamp(FlatBufferBuilder builder, long timestamp) { builder.addLong(2, timestamp, 0); }
  public static int endTaskState(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
}

