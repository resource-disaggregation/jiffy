package mmux;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import mmux.hadoop.fs.MMuxFileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;


public class MMuxFileSystemTest {

  @Rule
  public TestName testName = new TestName();

  private DirectoryServer directoryServer;
  private StorageServer storageServer1;
  private StorageServer storageServer2;
  private StorageServer storageServer3;

  public MMuxFileSystemTest() {
    directoryServer = new DirectoryServer(System.getProperty("mmux.directory.exec",
                                          "directoryd"));
    storageServer1 = new StorageServer(System.getProperty("mmux.storage.exec",
                                       "storaged"));
    storageServer2 = new StorageServer(System.getProperty("mmux.storage.exec",
                                       "storaged"));
    storageServer3 = new StorageServer(System.getProperty("mmux.storage.exec",
                                       "storaged"));
  }

  @Before
  public void setUp() {
    System.out.println("=> Running " + testName.getMethodName());
  }

  @After
  public void tearDown() throws InterruptedException {
    stopServers();
  }

  private void stopServers() throws InterruptedException {
    storageServer1.stop();
    storageServer2.stop();
    storageServer3.stop();
    directoryServer.stop();
  }

  private void startServers(boolean chain,
                            boolean autoScale) throws InterruptedException {

    try {
      directoryServer.start(this.getClass().getResource("/directory.conf").getFile());
    } catch (IOException e) {
      throw new InterruptedException(
        String.format("Error running executable %s: %s\n",
                      directoryServer.getExecutable(),
                      e.getMessage()));
    }

    String conf1 = autoScale ?
                   this.getClass().getResource("/storage_auto_scale.conf").getFile()
                   : this.getClass().getResource("/storage1.conf").getFile();
    try {
      storageServer1.start(conf1);
    } catch (IOException e) {
      throw new InterruptedException(
        String.format("Error running executable %s: %s\n",
                      storageServer1.getExecutable(),
                      e.getMessage()));
    }

    if (chain) {
      try {
        storageServer2.start(this.getClass().getResource("/storage2.conf").getFile());
      } catch (IOException e) {
        throw new InterruptedException(
          String.format("Error running executable %s: %s\n",
                        storageServer2.getExecutable(),
                        e.getMessage()));
      }

      try {
        storageServer3.start(this.getClass().getResource("/storage3.conf").getFile());
      } catch (IOException e) {
        throw new InterruptedException(
          String.format("Error running executable %s: %s\n",
                        storageServer3.getExecutable(),
                        e.getMessage()));
      }
    }
  }

  @Test
  public void testCreateWriteRenameReadFile() throws InterruptedException,
           TException,
    IOException {
    startServers(false, false);
    try (MMuxFileSystem fs = directoryServer.connect()) {

      // Create file
      Path file_path = new Path("testfile");
      FSDataOutputStream out_stream = fs.create(file_path);

      // Write string to file
      String data = "teststring";
      byte[] data_bytes = data.getBytes();
      out_stream.write(data_bytes, 0, data_bytes.length);
      out_stream.close();


      Path rename_path = new Path("testfile_renamed");
      fs.rename(file_path, rename_path);

      // Open created file
      FSDataInputStream in_stream = fs.open(rename_path, data_bytes.length);

      // Read all data from the file
      byte[] read_bytes = new byte[data_bytes.length];
      in_stream.readFully(0, read_bytes);

      // Ensure data read from file is the string we wrote to file.
      Assert.assertArrayEquals(data_bytes, read_bytes);
      Assert.assertEquals(data, new String(read_bytes));
      in_stream.close();

      // Clean up test
      fs.delete(rename_path, false);
    } finally {
      stopServers();
    }
  }

  @Test
  public void testMakeAndDeleteDir() throws InterruptedException, TException,
    IOException {
    startServers(false, false);
    try (MMuxFileSystem fs = directoryServer.connect()) {
      Path expected_wd = new Path("/fsdir");
      Assert.assertEquals(fs.getWorkingDirectory(), expected_wd);

      // Create a new directory and ls the base directory.
      fs.mkdirs(new Path("testdir"), FsPermission.getDefault());

      // Successfully list the directory
      FileStatus[] files = fs.listStatus(new Path("testdir"));
      Assert.assertEquals(0, files.length);

      // Clean up test
      fs.delete(new Path("testdir"), true);
    } finally {
      stopServers();
    }
  }
}
