package jiffy.storage;

import java.nio.ByteBuffer;
import java.util.HashMap;
import jiffy.util.ByteBufferUtils;

class HashTableCommands {
  static final ByteBuffer EXISTS = ByteBufferUtils.fromString("exists");
  static final ByteBuffer GET = ByteBufferUtils.fromString("get");
  static final ByteBuffer PUT = ByteBufferUtils.fromString("put");
  static final ByteBuffer REMOVE = ByteBufferUtils.fromString("remove");
  static final ByteBuffer UPDATE = ByteBufferUtils.fromString("update");
  static final ByteBuffer UPSERT = ByteBufferUtils.fromString("upsert");

  static final HashMap<ByteBuffer, CommandType> CMD_TYPES;
  static {
    CMD_TYPES = new HashMap<>();
    CMD_TYPES.put(HashTableCommands.PUT, CommandType.mutator);
    CMD_TYPES.put(HashTableCommands.GET, CommandType.accessor);
    CMD_TYPES.put(HashTableCommands.EXISTS, CommandType.accessor);
    CMD_TYPES.put(HashTableCommands.UPDATE, CommandType.mutator);
    CMD_TYPES.put(HashTableCommands.UPSERT, CommandType.mutator);
    CMD_TYPES.put(HashTableCommands.REMOVE, CommandType.mutator);
  }
}