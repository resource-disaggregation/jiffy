package jiffy.hadoop.fs.contract;

import java.io.Closeable;
import java.io.IOException;
import jiffy.StorageServer;
import jiffy.hadoop.fs.NameServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;

public class JiffyContract extends AbstractFSContract implements Closeable {

  private final NameServer nameServer;
  private final StorageServer storageServer;
  private static final String CONTRACT_XML = "contract/jfs.xml";
  private static final String TEST_PATH = "/test";

  /**
   * Constructor: loads the authentication keys if found
   *
   * @param conf configuration to work with
   */
  JiffyContract(Configuration conf) {
    super(conf);
    addConfResource(CONTRACT_XML);
    nameServer = new NameServer(System.getProperty("jiffy.directory.exec", "directoryd"));
    storageServer = new StorageServer(System.getProperty("jiffy.storage.exec", "storaged"));
  }

  @Override
  public void init() throws IOException {
    super.init();

    try {
      nameServer.start(getClass().getResource("/directory.conf").getFile());
      storageServer.start(getClass().getResource("/storage.conf").getFile());
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public FileSystem getTestFileSystem() {
    return nameServer.connectFS();
  }

  @Override
  public String getScheme() {
    return "jfs";
  }

  @Override
  public Path getTestPath() {
    return new Path(TEST_PATH);
  }

  @Override
  public void close() throws IOException {
    try {
      storageServer.stop();
      nameServer.stop();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }
}
