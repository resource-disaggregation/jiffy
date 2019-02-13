package jiffy.util;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class ByteBufferUtils {
  public static String toString(ByteBuffer buf) {
    int pos = buf.position();
    String out = StandardCharsets.UTF_8.decode(buf).toString();
    buf.position(pos);
    return out;
  }

  public static ByteBuffer fromString(String buf) {
    return StandardCharsets.UTF_8.encode(buf);
  }

  public static List<ByteBuffer> fromByteBuffers(ByteBuffer... args) {
    return Arrays.asList(args);
  }

}
