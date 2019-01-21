package jiffy.util;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ByteBufferUtils {
  public static String toString(ByteBuffer buf) {
    String out = StandardCharsets.UTF_8.decode(buf).toString();
    buf.rewind();
    return out;
  }

  public static ByteBuffer fromString(String buf) {
    return StandardCharsets.UTF_8.encode(buf);
  }

  public static List<ByteBuffer> fromStrings(String... args) {
    List<ByteBuffer> out = new ArrayList<>(args.length);
    for (String arg : args) {
      out.add(StandardCharsets.UTF_8.encode(arg));
    }
    return out;
  }

  public static List<ByteBuffer> fromStrings(List<String> args) {
    List<ByteBuffer> out = new ArrayList<>(args.size());
    for (String arg : args) {
      out.add(StandardCharsets.UTF_8.encode(arg));
    }
    return out;
  }

  public static List<String> toStrings(List<ByteBuffer> args) {
    List<String> out = new ArrayList<>(args.size());
    for (ByteBuffer arg : args) {
      out.add(StandardCharsets.UTF_8.decode(arg).toString());
      arg.rewind();
    }
    return out;
  }

  public static List<ByteBuffer> fromByteBuffers(ByteBuffer... args) {
    return Arrays.asList(args);
  }

}
