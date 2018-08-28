package mmux.hadoop.fs;

import java.io.IOException;
import mmux.StorageServer;
import org.apache.commons.lang.RandomStringUtils;
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

  private static final int DEFAULT_FILE_LENGTH = 8;

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

  private String getRandomFilename(int length) {
    return RandomStringUtils.randomAlphabetic(length);
  }

  @Test
  public void testCreateWriteRenameReadFile() throws InterruptedException, TException, IOException {
    startServers();
    try (MMuxFileSystem fs = nameServer.connectFS()) {

      // Create file
      String originalFilename = getRandomFilename(DEFAULT_FILE_LENGTH);
      String renamedFilename = getRandomFilename(DEFAULT_FILE_LENGTH);

      Path filePath = new Path(originalFilename);
      FSDataOutputStream out_stream = fs.create(filePath);

      // Write string to file
      String data = "teststring";
      byte[] dataBytes = data.getBytes();
      out_stream.write(dataBytes, 0, dataBytes.length);
      out_stream.close();

      Path renamePath = new Path(renamedFilename);
      fs.rename(filePath, renamePath);

      // Open created file
      FSDataInputStream in_stream = fs.open(renamePath, dataBytes.length);

      // Read all data from the file
      byte[] read_bytes = new byte[dataBytes.length];
      in_stream.readFully(0, read_bytes);

      // Ensure data read from file is the string we wrote to file.
      Assert.assertArrayEquals(dataBytes, read_bytes);
      Assert.assertEquals(data, new String(read_bytes));
      in_stream.close();

      // Clean up test
      fs.delete(renamePath, false);
    } finally {
      stopServers();
    }
  }

  @Test
  public void testMakeAndDeleteDir() throws InterruptedException, TException, IOException {
    /*
    startServers();
    try (MMuxFileSystem fs = nameServer.connectFS()) {
      Path expectedWorkingDirectory = new Path("/fsdir");
      Assert.assertEquals(fs.getWorkingDirectory(), expectedWorkingDirectory);

      // Create a new directory and ls the base directory
      String dirName = getRandomFilename(DEFAULT_FILE_LENGTH);
      fs.mkdirs(new Path(dirName), FsPermission.getDefault());

      // Successfully list the directory
      FileStatus[] files = fs.listStatus(new Path(dirName));
      Assert.assertEquals(0, files.length);

      // Clean up test
      fs.delete(new Path(dirName), true);
    } finally {
      stopServers();
    }
    */
  }

  @Test
  public void listStatusWithNestedDirectories() throws InterruptedException, IOException {
    startServers();
    try (MMuxFileSystem fs = nameServer.connectFS()) {
      String dirName = getRandomFilename(8);
      fs.mkdirs(new Path(dirName), FsPermission.getDefault());
      fs.mkdirs(new Path(dirName + "/" + getRandomFilename(8)), FsPermission.getDefault());

      /* Write a file */
      Path filePath = new Path(dirName + "/" + getRandomFilename(8));
      FSDataOutputStream out_stream = fs.create(filePath);

      // Write string to file
      String data = "teststring";
      byte[] dataBytes = data.getBytes();
      out_stream.write(dataBytes, 0, dataBytes.length);
      out_stream.close();

      FileStatus[] files = fs.listStatus(new Path(dirName));
      Assert.assertEquals(2, files.length);

      fs.delete(new Path(dirName), true);
    } finally {
      stopServers();
    }
  }

  void createFileWithText(MMuxFileSystem fs, Path p, String data) throws IOException {
    FSDataOutputStream out_stream = fs.create(p);
    byte[] dataBytes = data.getBytes();
    out_stream.write(dataBytes, 0, dataBytes.length);
    out_stream.close();
  }

  @Test
  public void bufferedReadFile() throws InterruptedException, IOException {
    startServers();
    try (MMuxFileSystem fs = nameServer.connectFS()) {
      Path filePath = new Path(getRandomFilename(8));
      String data = getRandomFilename(100);
      createFileWithText(fs, filePath, data);

      int buffSize = 8;
      FSDataInputStream in = fs.open(filePath);
      byte buf[] = new byte[buffSize];
      int totalBytesRead = 0;
      int bytesRead = in.read(buf);
      totalBytesRead += bytesRead;
      while (bytesRead >= 0) {
        bytesRead = in.read(buf);
        totalBytesRead += bytesRead;
      }

      Assert.assertEquals(data.getBytes().length, totalBytesRead);

    } finally {
      stopServers();
    }
  }
}
