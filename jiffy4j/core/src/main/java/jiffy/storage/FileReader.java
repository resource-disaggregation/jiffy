package jiffy.storage;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import jiffy.directory.directory_service.Client;
import jiffy.directory.rpc_data_status;
import jiffy.util.ByteBufferUtils;
import org.apache.thrift.TException;

public class FileReader extends FileClient {

  public FileReader(Client fs, String path,
      rpc_data_status dataStatus, int timeoutMs) throws TException {
    super(fs, path, dataStatus, timeoutMs);
  }

  @Override
  ByteBuffer handleRedirect(List<ByteBuffer> args, ByteBuffer response)
      throws TException {
    boolean readFlag = true;
    String responseStr = ByteBufferUtils.toString(response);
    if (responseStr.equals("!redo")) {
      return null;
    }

    if (responseStr.startsWith("!split_read")) {
      StringBuilder resultStr = new StringBuilder();
      while (responseStr.startsWith("!split_read")) {
        String[] parts = responseStr.split("!");
        String firstSplit = parts[parts.length - 1];
        int remainingDataLength = ByteBufferUtils.toInt(args.get(1)) - firstSplit.length();
        resultStr.append(firstSplit);

        if (partition >= blocks.size() - 1)
          addNewBlock(parts);

        partition++;
        updateLastPartition(partition);
        offset = 0;

        List<ByteBuffer> newArgs = Arrays
            .asList(FileCommands.READ, ByteBufferUtils.fromLong(offset),
                ByteBufferUtils.fromInteger(remainingDataLength));
        response = blocks.get(partition).runCommand(newArgs).get(0);
        responseStr = ByteBufferUtils.toString(response);
        if (!responseStr.equals("!msg_not_found")) {
          if (responseStr.startsWith("!split_read")) {
            continue;
          }
          offset += responseStr.length();
          resultStr.append(responseStr);
        }
        readFlag = false;
      }
      responseStr = resultStr.toString();
      response = ByteBufferUtils.fromString(responseStr);
    }

    if (!responseStr.equals("!msg_not_found") && readFlag) {
      offset += responseStr.length();
    }
    return response;
  }

  public ByteBuffer read(int size) throws TException {
    List<ByteBuffer> args = ByteBufferUtils
        .fromByteBuffers(FileCommands.READ, ByteBufferUtils.fromLong(offset),
            ByteBufferUtils.fromInteger(size));
    ByteBuffer response = null;
    while (response == null) {
      response = blocks.get(partition).runCommand(args).get(0);
      response = handleRedirect(args, response);
    }
    return response;
  }
}
