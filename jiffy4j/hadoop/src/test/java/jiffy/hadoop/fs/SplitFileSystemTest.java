package jiffy.hadoop.fs;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class SplitFileSystemTest {

  @Rule
  public TestName testName = new TestName();

  private static final String EPHEMERAL_FILE = "tmp/nested/ephemeral.txt";
  private static final String PERSISTENT_FILE = "test/persistent.txt";

  private static final String EPHEMERAL_BASE_DIR = "tmp/ephemeral/";
  private static final String PERSISTENT_BASE_DIR = "test/persistent/";

  private SplitFileSystem splitFS;

  private static Configuration getTestConfiguration() {
    Configuration conf = new Configuration();
    conf.set("splitfs.persistent.fs", "file:///");
    conf.set("splitfs.ephemeral.fs", "file:///");
    conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
    return conf;
  }

  @Before
  public void setUp() throws URISyntaxException, IOException {
    System.out.println("=> Running " + testName.getMethodName());

    // Create conf with local file system for both ephemeral and persistent.
    Configuration conf = getTestConfiguration();

    splitFS = new SplitFileSystem();
    splitFS.initialize(new URI("splitfs:///"), conf);

    splitFS.mkdirs(new Path(EPHEMERAL_BASE_DIR));
    splitFS.mkdirs(new Path(PERSISTENT_BASE_DIR));
    splitFS.setEphemeralWorkingDirectory(new Path(EPHEMERAL_BASE_DIR));
    splitFS.setPersistentWorkingDirectory(new Path(PERSISTENT_BASE_DIR));
  }

  @After
  public void tearDown() throws IOException {
    splitFS.delete(new Path("tmp"), true);
    splitFS.delete(new Path("test"), true);
  }

  @Test
  public void testWorkingDirectories() {
    Assert.assertEquals(new Path(EPHEMERAL_BASE_DIR).getName(),
        splitFS.getEphemeralWorkingDirectory().getName());
    Assert.assertEquals(new Path(PERSISTENT_BASE_DIR).getName(),
        splitFS.getPersistentWorkingDirectory().getName());
  }

  @Test
  public void testCreateEphemeralFile() throws IOException {
    Assert.assertNotNull(splitFS);

    // Create an ephemeral test file.
    Path p = new Path(EPHEMERAL_FILE);
    System.out.println("ephemeralFile: " + EPHEMERAL_FILE);
    FSDataOutputStream f = splitFS.create(p);
    f.writeChars("Hello");
    f.close();

    // Check that the file exists in the ephemeral directory
    File localFile = new File(EPHEMERAL_BASE_DIR + "/" + EPHEMERAL_FILE);
    System.out.println("localFile: " + localFile.getAbsolutePath());
    Assert.assertTrue(localFile.exists());
  }

  @Test
  public void testCreatePersistentFile() throws IOException {
    Assert.assertNotNull(splitFS);

    // Create a test file with the persistent file prefix.
    Path p = new Path(PERSISTENT_FILE);
    FSDataOutputStream f = splitFS.create(p);
    f.writeChars("Hello");
    f.close();

    // Check that the file exists in the persistent directory
    File localFile = new File(PERSISTENT_BASE_DIR + "/" + PERSISTENT_FILE);
    Assert.assertTrue(localFile.exists());
  }
}
