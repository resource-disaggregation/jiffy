package jiffy.storage;

import java.nio.ByteBuffer;
import java.util.HashMap;
import jiffy.util.ByteBufferUtils;

class FileCommands {
  static final ByteBuffer WRITE = ByteBufferUtils.fromString("write");
  static final ByteBuffer READ = ByteBufferUtils.fromString("read");
  static final ByteBuffer SEEK = ByteBufferUtils.fromString("seek");

  static final HashMap<ByteBuffer, CommandType> CMD_TYPES;
  static {
    CMD_TYPES = new HashMap<>();
    CMD_TYPES.put(WRITE, CommandType.mutator);
    CMD_TYPES.put(READ, CommandType.accessor);
    CMD_TYPES.put(SEEK, CommandType.accessor);
  }
}
