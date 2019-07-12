package jiffy.storage;

import java.nio.ByteBuffer;
import java.util.List;
import jiffy.directory.directory_service.Client;
import jiffy.directory.rpc_data_status;
import org.apache.thrift.TException;

public abstract class DataStructureClient {
  protected Client fs;
  protected String path;
  protected rpc_data_status dataStatus;
  protected BlockClientCache cache;

  DataStructureClient(Client fs, String path, rpc_data_status dataStatus, int timeoutMs) {
    this.fs = fs;
    this.path = path;
    this.dataStatus = dataStatus;
    this.cache = new BlockClientCache(timeoutMs);
  }

  abstract void refresh() throws TException;

  abstract ByteBuffer handleRedirect(int cmdId, List<ByteBuffer> args, ByteBuffer response)
      throws TException;

  abstract List<ByteBuffer> handleRedirects(int cmdId, List<ByteBuffer> args,
      List<ByteBuffer> responses) throws TException;

  public rpc_data_status status() {
    return dataStatus;
  }
}
