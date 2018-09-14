package mmux.util;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ByteBufferUtils {
  public static String toString(ByteBuffer buf) {
    String response = StandardCharsets.UTF_8.decode(buf).toString();
    buf.rewind();
    return response;
  }

  public static ByteBuffer fromString(String buf) {
    return StandardCharsets.UTF_8.encode(buf);
  }

  public static List<ByteBuffer> fromByteBuffers(ByteBuffer... args) {
    return Arrays.asList(args);
  }

}
