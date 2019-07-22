package jiffy.storage;

import java.nio.ByteBuffer;
import java.util.List;
import jiffy.directory.directory_service.Client;
import jiffy.directory.rpc_data_status;
import org.apache.thrift.TException;

public abstract class DataStructureClient {
  protected Client fs;
  protected String path;
  rpc_data_status dataStatus;
  BlockClientCache cache;

  DataStructureClient(Client fs, String path, rpc_data_status dataStatus, int timeoutMs) {
    this.fs = fs;
    this.path = path;
    this.dataStatus = dataStatus;
    this.cache = new BlockClientCache(timeoutMs);
  }

  abstract void refresh() throws TException;

  abstract ByteBuffer handleRedirect(List<ByteBuffer> args, ByteBuffer response) throws TException;

  public rpc_data_status status() {
    return dataStatus;
  }
}
