package jiffy.storage;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import jiffy.directory.directory_service.Client;
import jiffy.directory.rpc_data_status;
import jiffy.util.ByteBufferUtils;
import org.apache.thrift.TException;

public class FileWriter extends FileClient {

  public FileWriter(Client fs, String path,
      rpc_data_status dataStatus, int timeoutMs) throws TException {
    super(fs, path, dataStatus, timeoutMs);
  }

  public ByteBuffer write(ByteBuffer buf) throws TException {
    List<ByteBuffer> args = ByteBufferUtils
        .fromByteBuffers(FileCommands.WRITE, buf, ByteBufferUtils.fromLong(offset));
    ByteBuffer response = null;
    while (response == null) {
      response = blocks.get(partition).runCommand(args).get(0);
      response = handleRedirect(args, response);
    }
    return response;
  }

  @Override
  ByteBuffer handleRedirect(List<ByteBuffer> args, ByteBuffer response)
      throws TException {
    boolean writeFlag = true;
    String responseStr = ByteBufferUtils.toString(response);

    if (responseStr.equals("!redo")) return null;

    while (responseStr.startsWith("!split_write")) {
      String[] parts = responseStr.split("!");
      int remainingDataLength = Integer.parseInt(parts[parts.length - 1]);

      // TODO: remove conversion to and from string (stick to bytebuffers)
      String data = ByteBufferUtils.toString(args.get(0));
      String remainingData = data.substring(data.length() - remainingDataLength);

      if (partition >= blocks.size() - 1) {
        addNewBlock(parts);
      }

      partition++;
      updateLastPartition(partition);
      offset = 0;

      List<ByteBuffer> newArgs = Arrays
          .asList(FileCommands.WRITE, ByteBufferUtils.fromString(remainingData),
              ByteBufferUtils.fromLong(offset));
      do {
        response = blocks.get(partition).runCommand(newArgs).get(0);
        responseStr = ByteBufferUtils.toString(response);
      } while (responseStr.equals("!redo"));
      offset += remainingDataLength;
      writeFlag = false;
    }

    if (writeFlag) {
      offset += args.get(1).array().length;
    }
    return response;
  }
}
