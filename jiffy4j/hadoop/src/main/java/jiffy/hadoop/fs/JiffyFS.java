package jiffy.hadoop.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Progressable;

public class JiffyFS extends AbstractFileSystem {
  private JiffyFileSystem dfs;

  public JiffyFS(final URI uri, final Configuration conf) throws IOException, URISyntaxException {
    super(uri, "jfs", true, 9090);
    this.dfs = new JiffyFileSystem();
    dfs.initialize(uri, conf);
  }

  @Override
  public int getUriDefaultPort() {
    return 9090;
  }

  @Override
  public void checkPath(Path path) {
    //Overridden to accept paths despite splitfs:///
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    return dfs.getServerDefaults(dfs.getWorkingDirectory());
  }

  @Override
  public FSDataOutputStream createInternal(Path path, EnumSet<CreateFlag> flag, FsPermission absolutePermission, int bufferSize, short replication, long blockSize, Progressable progress, ChecksumOpt checksumOpt, boolean createParent) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, UnsupportedFileSystemException, UnresolvedLinkException, IOException {
    return dfs.create(path, absolutePermission, false, bufferSize, replication, blockSize, progress);
  }

  @Override
  public void mkdir(Path path, FsPermission permission, boolean createParent) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, UnresolvedLinkException, IOException {
    dfs.mkdirs(path, permission);
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
    return dfs.delete(path, recursive);
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
    return dfs.open(path, bufferSize);
  }

  @Override
  public boolean setReplication(Path f, short replication) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
    return dfs.setReplication(f, replication);
  }

  @Override
  public void renameInternal(Path src, Path dst) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, UnresolvedLinkException, IOException {
    dfs.rename(src, dst);
  }

  @Override
  public void setPermission(Path f, FsPermission permission) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
    dfs.setPermission(f, permission);
  }

  @Override
  public void setOwner(Path f, String username, String groupname) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
  }

  @Override
  public void setTimes(Path f, long mtime, long atime) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
  }

  @Override
  public FileChecksum getFileChecksum(Path f) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
    return null;
  }

  @Override
  public FileStatus getFileStatus(Path path) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
    return dfs.getFileStatus(path);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(Path path, long start, long len) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
    return dfs.getFileBlockLocations(path, start, len);
  }

  @Override
  public FsStatus getFsStatus() throws AccessControlException, FileNotFoundException, IOException {
    return dfs.getStatus();
  }

  @Override
  public FileStatus[] listStatus(Path path) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
    return dfs.listStatus(path);
  }

  @Override
  public void setVerifyChecksum(boolean verifyChecksum) throws AccessControlException, IOException {
    dfs.setVerifyChecksum(verifyChecksum);
  }
}