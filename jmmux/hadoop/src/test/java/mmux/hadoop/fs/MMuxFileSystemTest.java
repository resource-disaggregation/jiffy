package mmux.hadoop.fs;

import java.io.IOException;
import mmux.StorageServer;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
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

  private static final int FILENAME_LENGTH = 8;
  private static final String TEST_STRING = "teststring";

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
      throw new InterruptedException(nameServer.getExecutable() + ": " + e.getMessage());
    }

    try {
      storageServer.start(this.getClass().getResource("/storage.conf").getFile());
    } catch (IOException e) {
      throw new InterruptedException(storageServer.getExecutable() + ": " + e.getMessage());
    }

  }

  private String randomFilename() {
    return RandomStringUtils.randomAlphabetic(MMuxFileSystemTest.FILENAME_LENGTH);
  }

  @Test
  public void testCreateWriteRenameReadFile() throws InterruptedException, IOException {
    startServers();
    try (MMuxFileSystem fs = nameServer.connectFS()) {

      // Create file
      String originalFilename = randomFilename();
      String renamedFilename = randomFilename();

      Path filePath = new Path(originalFilename);
      FSDataOutputStream out = fs.create(filePath);

      // Write string to file
      byte[] dataBytes = TEST_STRING.getBytes();
      out.write(dataBytes, 0, dataBytes.length);
      out.close();

      Path renamePath = new Path(renamedFilename);
      fs.rename(filePath, renamePath);

      // Open created file
      FSDataInputStream in = fs.open(renamePath, dataBytes.length);

      // Read all data from the file
      byte[] readBytes = new byte[dataBytes.length];
      in.readFully(0, readBytes);

      // Ensure data read from file is the string we wrote to file.
      Assert.assertArrayEquals(dataBytes, readBytes);
      Assert.assertEquals(TEST_STRING, new String(readBytes));
      in.close();

      // Clean up test
      fs.delete(renamePath, false);
    } finally {
      stopServers();
    }
  }

  @Test
  public void testMakeAndDeleteDir() throws InterruptedException, IOException {
    startServers();
    try (MMuxFileSystem fs = nameServer.connectFS()) {
      Path expectedWorkingDirectory = new Path("/fsdir");
      Assert.assertEquals(fs.getWorkingDirectory(), expectedWorkingDirectory);

      // Create a new directory and ls the base directory
      String dirName = randomFilename();
      fs.mkdirs(new Path(dirName), FsPermission.getDefault());

      // Successfully list the directory
      FileStatus[] files = fs.listStatus(new Path(dirName));
      Assert.assertEquals(0, files.length);

      // Clean up test
      fs.delete(new Path(dirName), true);
    } finally {
      stopServers();
    }
  }

  @Test
  public void listStatusWithNestedDirectories() throws InterruptedException, IOException {
    startServers();
    try (MMuxFileSystem fs = nameServer.connectFS()) {
      String dirName = randomFilename();
      fs.mkdirs(new Path(dirName), FsPermission.getDefault());
      fs.mkdirs(new Path(dirName + "/" + randomFilename()),
          FsPermission.getDefault());

      /* Create a file */
      Path filePath = new Path(dirName + "/" + randomFilename());
      FSDataOutputStream out = fs.create(filePath);

      // Write string to file
      byte[] dataBytes = TEST_STRING.getBytes();
      out.write(dataBytes, 0, dataBytes.length);
      out.close();

      FileStatus[] files = fs.listStatus(new Path(dirName));
      Assert.assertEquals(2, files.length);

      fs.delete(new Path(dirName), true);
    } finally {
      stopServers();
    }
  }
}
