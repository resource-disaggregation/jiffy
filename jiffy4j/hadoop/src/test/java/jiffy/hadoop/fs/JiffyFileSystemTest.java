package jiffy.hadoop.fs;

import java.io.IOException;
import jiffy.StorageServer;
import org.apache.commons.lang.ArrayUtils;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JiffyFileSystemTest {

  private static final Logger LOG = LoggerFactory.getLogger(JiffyFileSystemTest.class);

  @Rule
  public TestName testName = new TestName();

  private NameServer nameServer;
  private StorageServer storageServer;

  private static final int FILENAME_LENGTH = 8;
  private static final String TEST_STRING = "teststring";

  public JiffyFileSystemTest() {
    nameServer = new NameServer(System.getProperty("jiffy.directory.exec", "directoryd"));
    storageServer = new StorageServer(System.getProperty("jiffy.storage.exec", "storaged"));
  }

  private static String formatTestDescription(String s) {
    return s.replaceAll(
        String.format("%s|%s|%s",
            "(?<=[A-Z])(?=[A-Z][a-z])",
            "(?<=[^A-Z])(?=[A-Z])",
            "(?<=[A-Za-z])(?=[^A-Za-z])"
        ),
        " "
    ).toLowerCase();
  }

  @Before
  public void setUp() {
    LOG.info(formatTestDescription(testName.getMethodName()));
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
    return RandomStringUtils.randomAlphabetic(JiffyFileSystemTest.FILENAME_LENGTH);
  }

  private String randomData(int dataLength) {
    return RandomStringUtils.randomAlphabetic(dataLength);
  }

  @Test
  public void testCreateWriteRenameReadFile() throws InterruptedException, IOException {
    startServers();
    try (JiffyFileSystem fs = nameServer.connectFS()) {

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
    try (JiffyFileSystem fs = nameServer.connectFS()) {
      Path expectedWorkingDirectory = new Path("/");
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
    try (JiffyFileSystem fs = nameServer.connectFS()) {
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

  private void createFileWithData(JiffyFileSystem fs, Path p, byte[] data) throws IOException {
    FSDataOutputStream out = fs.create(p);
    out.write(data, 0, data.length);
    out.close();
  }

  private void createFileWithData(JiffyFileSystem fs, Path p, byte[] data, long blockSize)
      throws IOException {
    FSDataOutputStream out = fs.create(p, false, 0, (short) 1, blockSize);
    out.write(data, 0, data.length);
    out.close();
  }

  @Test
  public void bufferedReadFile() throws InterruptedException, IOException {
    startServers();
    try (JiffyFileSystem fs = nameServer.connectFS()) {
      Path filePath = new Path(randomFilename());
      int dataLength = 80;
      byte[] data = randomData(dataLength).getBytes();
      createFileWithData(fs, filePath, data);

      int buffSize = 8;
      Assert.assertEquals(data.length, testRead(data, fs.open(filePath), new byte[buffSize]));

    } finally {
      stopServers();
    }
  }

  @Test
  public void readFileFully() throws InterruptedException, IOException {
    startServers();
    try (JiffyFileSystem fs = nameServer.connectFS()) {
      Path filePath = new Path(randomFilename());
      int dataLength = 80;
      byte[] data = randomData(dataLength).getBytes();
      createFileWithData(fs, filePath, data);

      FSDataInputStream in = fs.open(filePath);
      byte[] buf = new byte[dataLength];
      in.readFully(0, buf);
      Assert.assertArrayEquals(data, buf);

    } finally {
      stopServers();
    }
  }

  @Test
  public void fileDoesNotExist() throws InterruptedException, IOException {
    startServers();
    try (JiffyFileSystem fs = nameServer.connectFS()) {
      Assert.assertFalse(fs.exists(new Path(randomFilename())));

    } finally {
      stopServers();
    }
  }

  @Test
  public void seekThenRead() throws InterruptedException, IOException {
    startServers();
    try (JiffyFileSystem fs = nameServer.connectFS()) {
      Path filePath = new Path(randomFilename());
      int dataLength = 80;
      byte[] data = randomData(dataLength).getBytes();
      createFileWithData(fs, filePath, data);

      FSDataInputStream in = fs.open(filePath);
      in.seek(40);
      byte[] buf = new byte[40];
      int bytesRead = in.read(buf, 0, 40);
      Assert.assertEquals(40, bytesRead);

      byte[] targetSlice = ArrayUtils.subarray(data, 40, 80);
      Assert.assertArrayEquals(buf, targetSlice);

    } finally {
      stopServers();
    }
  }

  @Test
  public void leasePathsOnRenameAndDelete() throws InterruptedException, IOException {
    startServers();
    try (JiffyFileSystem fs = nameServer.connectFS()) {
      // Files
      Path oldPath = new Path(randomFilename());
      FSDataOutputStream os = fs.create(oldPath);

      Path newPath = new Path(randomFilename());
      fs.rename(oldPath, newPath);
      Assert.assertTrue(fs.getClient().getWorker().hasPath("/" + newPath.toString()));
      Assert.assertFalse(fs.getClient().getWorker().hasPath("/" + oldPath.toString()));

      fs.delete(newPath, false);
      Assert.assertFalse(fs.getClient().getWorker().hasPath("/" + newPath.toString()));
      os.close();

      // Directories
      String fileName = randomFilename();
      oldPath = new Path("/test");
      os = fs.create(new Path("/test/" + fileName));

      newPath = new Path("/testRenamed");
      fs.rename(oldPath, newPath);
      Assert.assertTrue(fs.getClient().getWorker().hasPath("/testRenamed/" + fileName));
      Assert.assertFalse(fs.getClient().getWorker().hasPath("/test/" + fileName));

      fs.delete(new Path("/testRenamed"), true);
      Assert.assertFalse(fs.getClient().getWorker().hasPath("/testRenewed/" + fileName));
      os.close();
    } finally {
      stopServers();
    }
  }

  @Test
  public void testMultipleBlocks() throws InterruptedException, IOException {
    startServers();
    try (JiffyFileSystem fs = nameServer.connectFS()) {
      Path filePath = new Path(randomFilename());
      int dataLength = 5000;
      byte[] data = randomData(dataLength).getBytes();
      createFileWithData(fs, filePath, data,1024);
      int buffSize = 64;
      Assert.assertEquals(data.length, testRead(data, fs.open(filePath), new byte[buffSize]));
    } finally {
      stopServers();
    }
  }

  private int testRead(byte[] data, FSDataInputStream in, byte[] buf) throws IOException {
    int totalBytesRead = 0;
    byte[] targetSlice;
    int bytesRead;
    do {
      bytesRead = in.read(buf);
      if (bytesRead > 0) {
        targetSlice = ArrayUtils.subarray(data, totalBytesRead, totalBytesRead + bytesRead);
        Assert.assertArrayEquals(targetSlice, ArrayUtils.subarray(buf, 0, bytesRead));
        totalBytesRead += bytesRead;
      }
    } while (bytesRead >= 0);
    return totalBytesRead;
  }
}
