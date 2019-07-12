package jiffy.storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import jiffy.directory.directory_service.Client;
import jiffy.directory.rpc_data_status;
import jiffy.directory.rpc_replica_chain;
import jiffy.directory.rpc_storage_mode;
import jiffy.util.ByteBufferUtils;
import org.apache.thrift.TException;

public class FileClient extends DataStructureClient {

  private int readPartition;
  private long readOffset;

  private int writePartition;
  private long writeOffset;

  private int lastPartition;

  private List<ReplicaChainClient> blocks;

  public FileClient(Client fs, String path, rpc_data_status dataStatus, int timeoutMs) throws TException {
    super(fs, path, dataStatus, timeoutMs);

    blocks = new ArrayList<>(dataStatus.data_blocks.size());
    readOffset = 0;
    readPartition = 0;
    writeOffset = 0;
    writePartition = 0;
    lastPartition = 0;
    for (rpc_replica_chain chain : dataStatus.data_blocks) {
      blocks.add(new ReplicaChainClient(fs, path, cache, chain));
    }
  }

  @Override
  void refresh() throws TException {
    dataStatus = fs.dstatus(path);
    blocks.clear();
    for (rpc_replica_chain chain : dataStatus.data_blocks) {
      blocks.add(new ReplicaChainClient(fs, path, cache, chain));
    }
  }

  @Override
  ByteBuffer handleRedirect(int op, List<ByteBuffer> args, ByteBuffer response)
      throws TException {
    boolean readFlag = true;
    boolean writeFlag = true;
    String responseStr = ByteBufferUtils.toString(response);
    if (responseStr.equals("!redo")) {
      return null;
    }

    if (op == FileOps.WRITE) {
      while (responseStr.startsWith("!split_write")) {
        String[] parts = responseStr.split("!");
        int remainingDataLength = Integer.parseInt(parts[parts.length - 1]);

        // TODO: remove conversion to and from string (stick to bytebuffers)
        String data = ByteBufferUtils.toString(args.get(0));
        String remainingData = data.substring(data.length() - remainingDataLength);
        addNewBlockIfNeeded(op, parts);
        writePartition++;
        updateLastPartition(writePartition);
        writeOffset = 0;

        List<ByteBuffer> newArgs = Arrays
            .asList(ByteBufferUtils.fromString(remainingData),
                ByteBufferUtils.fromLong(writeOffset));
        do {
          response = blocks.get(blockId(FileOps.WRITE)).runCommand(FileOps.WRITE, newArgs).get(0);
          responseStr = ByteBufferUtils.toString(response);
        } while (responseStr.equals("!redo"));
        writeOffset += remainingDataLength;
        writeFlag = false;
      }
    }

    if (op == FileOps.READ && responseStr.startsWith("!split_read")) {
      StringBuilder resultStr = new StringBuilder();
      while (responseStr.startsWith("!split_read")) {
        String[] parts = responseStr.split("!");
        String firstSplit = parts[parts.length - 1];
        int remainingDataLength = ByteBufferUtils.toInt(args.get(1)) - firstSplit.length();
        resultStr.append(firstSplit);

        addNewBlockIfNeeded(op, parts);
        readPartition++;
        updateLastPartition(readPartition);
        readOffset = 0;

        List<ByteBuffer> newArgs = Arrays
            .asList(ByteBufferUtils.fromLong(readOffset),
                ByteBufferUtils.fromInteger(remainingDataLength));
        response = blocks.get(blockId(op)).runCommand(op, newArgs).get(0);
        responseStr = ByteBufferUtils.toString(response);
        if (!responseStr.equals("!msg_not_found")) {
          if (responseStr.startsWith("!split_read")) {
            continue;
          }
          readOffset += responseStr.length();
          resultStr.append(responseStr);
        }
        readFlag = false;
      }
      responseStr = resultStr.toString();
      response = ByteBufferUtils.fromString(responseStr);
    }

    if (!responseStr.equals("!msg_not_found") && op == FileOps.READ && readFlag) {
      readOffset += responseStr.length();
    }

    if (op == FileOps.WRITE && writeFlag) {
      writeOffset += args.get(0).array().length;
    }
    return response;
  }

  private void addNewBlockIfNeeded(int op, String[] parts) throws TException {
    if (needChain(op)) {
      List<String> chainList = Arrays.asList(Arrays.copyOfRange(parts, 2, parts.length - 1));
      rpc_replica_chain chain = new rpc_replica_chain(chainList, "", "",
          rpc_storage_mode.rpc_in_memory);
      blocks.add(new ReplicaChainClient(fs, path, cache, chain));
    }
  }

  @Override
  List<ByteBuffer> handleRedirects(int cmdId, List<ByteBuffer> args, List<ByteBuffer> responses)
      throws TException {
    return null;
  }

  public ByteBuffer write(ByteBuffer buf) throws TException {
    List<ByteBuffer> args = ByteBufferUtils
        .fromByteBuffers(buf, ByteBufferUtils.fromLong(writeOffset));
    ByteBuffer response = null;
    while (response == null) {
      response = blocks.get(blockId(FileOps.WRITE)).runCommand(FileOps.WRITE, args).get(0);
      response = handleRedirect(FileOps.WRITE, args, response);
    }
    return response;
  }

  public ByteBuffer read(int size) throws TException {
    List<ByteBuffer> args = ByteBufferUtils
        .fromByteBuffers(ByteBufferUtils.fromLong(readOffset), ByteBufferUtils.fromInteger(size));
    ByteBuffer response = null;
    while (response == null) {
      response = blocks.get(blockId(FileOps.READ)).runCommand(FileOps.READ, args).get(0);
      response = handleRedirect(FileOps.READ, args, response);
    }
    return response;
  }

  public boolean seek(long offset) throws TException {
    int seekPartition = blockId(FileOps.SEEK);
    List<ByteBuffer> ret = blocks.get(seekPartition)
        .runCommand(FileOps.SEEK, Collections.emptyList());
    long size = Long.parseLong(ByteBufferUtils.toString(ret.get(0)));
    long cap = Long.parseLong(ByteBufferUtils.toString(ret.get(1)));
    if (offset <= seekPartition * cap + size) {
      writePartition = readPartition = (int) (offset / cap);
      writeOffset = readOffset = offset % cap;
      return true;
    }
    return false;
  }

  private boolean needChain(int op) {
    if (op == FileOps.WRITE) {
      return writePartition >= blocks.size() - 1;
    } else if (op == FileOps.READ) {
      return readPartition >= blocks.size() - 1;
    } else {
      throw new IllegalStateException("Chain should only be added for read or write");
    }
  }

  private boolean isInvalid(int partitionNum) {
    return partitionNum >= blocks.size();
  }

  private void updateLastPartition(int partition) {
    if (lastPartition < partition) {
      lastPartition = partition;
    }
  }

  private int blockId(int op) {
    switch (op) {
      case FileOps.WRITE:
        if (isInvalid(writePartition)) {
          throw new IllegalStateException("Insufficient number of blocks, need to add more");
        }
        return writePartition;
      case FileOps.READ:
        if (isInvalid(readPartition)) {
          throw new IllegalStateException("Insufficient number of blocks, need to add more");
        }
        return readPartition;
      case FileOps.SEEK:
        return lastPartition;
      default:
        throw new IllegalStateException("blockId() called from invalid operation: " + op);
    }
  }
}
