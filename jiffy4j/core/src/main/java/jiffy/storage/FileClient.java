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

public abstract class FileClient extends DataStructureClient {

  int partition;
  long offset;

  private int lastPartition;

  List<ReplicaChainClient> blocks;

  FileClient(Client fs, String path, rpc_data_status dataStatus, int timeoutMs)
      throws TException {
    super(fs, path, dataStatus, timeoutMs);

    blocks = new ArrayList<>(dataStatus.data_blocks.size());
    offset = 0;
    partition = 0;
    lastPartition = 0;
    init();
  }

  private void init() throws TException {
    blocks.clear();
    for (rpc_replica_chain chain : dataStatus.data_blocks) {
      blocks.add(new ReplicaChainClient(fs, path, cache, chain, FileCommands.CMD_TYPES));
    }
  }

  @Override
  void refresh() throws TException {
    dataStatus = fs.dstatus(path);
    init();
  }

  void addNewBlock(String[] parts) throws TException {
    List<String> chainList = Arrays.asList(Arrays.copyOfRange(parts, 2, parts.length - 1));
    rpc_replica_chain chain = new rpc_replica_chain(chainList, "", "",
        rpc_storage_mode.rpc_in_memory);
    blocks.add(new ReplicaChainClient(fs, path, cache, chain, FileCommands.CMD_TYPES));
  }

  public boolean seek(long offset) throws TException {
    List<ByteBuffer> ret = blocks.get(lastPartition)
        .runCommand(Collections.singletonList(FileCommands.SEEK));
    long size = Long.parseLong(ByteBufferUtils.toString(ret.get(0)));
    long cap = Long.parseLong(ByteBufferUtils.toString(ret.get(1)));
    if (offset <= lastPartition * cap + size) {
      this.partition = (int) (offset / cap);
      this.offset = offset % cap;
      return true;
    }
    return false;
  }

  void updateLastPartition(int partition) {
    if (lastPartition < partition) {
      lastPartition = partition;
    }
  }

}
