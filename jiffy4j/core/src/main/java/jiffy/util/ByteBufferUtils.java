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

  public static ByteBuffer fromInteger(int i) {
    return fromString(Integer.toString(i));
  }

  public static ByteBuffer fromLong(long i) {
    return fromString(Long.toString(i));
  }

  public static long toLong(ByteBuffer buf) {
    return Long.parseLong(toString(buf));
  }

  public static int toInt(ByteBuffer buf) {
    return Integer.parseInt(toString(buf));
  }

  public static List<ByteBuffer> fromByteBuffers(ByteBuffer... args) {
    return Arrays.asList(args);
  }

}
