package mmux.hadoop.fs;

import java.io.IOException;
import mmux.StorageServer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
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

  private NameServer nameServer;
  private StorageServer storageServer;

  public MMuxFileSystemTest() {
    nameServer = new NameServer(System.getProperty("mmux.directory.exec", "directoryd"));
    storageServer = new StorageServer(System.getProperty("mmux.storage.exec", "storaged"));
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
    storageServer.stop();
    nameServer.stop();
  }

  private void startServers() throws InterruptedException {
    try {
      nameServer.start(this.getClass().getResource("/directory.conf").getFile());
    } catch (IOException e) {
      throw new InterruptedException(
          String.format("Error running executable %s: %s\n", nameServer.getExecutable(),
              e.getMessage()));
    }

    try {
      storageServer.start(this.getClass().getResource("/storage.conf").getFile());
    } catch (IOException e) {
      throw new InterruptedException(
          String.format("Error running executable %s: %s\n", storageServer.getExecutable(),
              e.getMessage()));
    }

  }

  @Test
  public void testCreateWriteRenameReadFile() throws InterruptedException, TException, IOException {
    startServers();
    try (MMuxFileSystem fs = nameServer.connectFS()) {

      // Create file
      Path filePath = new Path("testfile");
      FSDataOutputStream out_stream = fs.create(filePath);

      // Write string to file
      String data = "teststring";
      byte[] dataBytes = data.getBytes();
      out_stream.write(dataBytes, 0, dataBytes.length);
      out_stream.close();

      Path rename_path = new Path("testfile_renamed");
      fs.rename(filePath, rename_path);

      // Open created file
      FSDataInputStream in_stream = fs.open(rename_path, dataBytes.length);

      // Read all data from the file
      byte[] read_bytes = new byte[dataBytes.length];
      in_stream.readFully(0, read_bytes);

      // Ensure data read from file is the string we wrote to file.
      Assert.assertArrayEquals(dataBytes, read_bytes);
      Assert.assertEquals(data, new String(read_bytes));
      in_stream.close();

      // Clean up test
      fs.delete(rename_path, false);
    } finally {
      stopServers();
    }
  }

  @Test
  public void testMakeAndDeleteDir() throws InterruptedException, TException, IOException {
    startServers();
    try (MMuxFileSystem fs = nameServer.connectFS()) {
      Path expectedWorkingDirectory = new Path("/fsdir");
      Assert.assertEquals(fs.getWorkingDirectory(), expectedWorkingDirectory);

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
