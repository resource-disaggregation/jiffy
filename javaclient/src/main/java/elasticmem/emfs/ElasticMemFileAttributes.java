package elasticmem.emfs;

import elasticmem.directory.directory_service;
import elasticmem.directory.rpc_data_status;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import org.apache.thrift.TException;

public class ElasticMemFileAttributes implements BasicFileAttributes {

  private final directory_service.Client client;
  private final String path;

  ElasticMemFileAttributes(directory_service.Client client, String path) {
    this.client = client;
    this.path = path;
  }

  public rpc_data_status dataStatus() {
    try {
      return client.dstatus(path);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  public int permissions() {
    try {
      return client.getPermissions(path);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public FileTime lastModifiedTime() {
    try {
      return FileTime.fromMillis(client.lastWriteTime(path));
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public FileTime lastAccessTime() {
    return lastModifiedTime();
  }

  @Override
  public FileTime creationTime() {
    return lastModifiedTime();
  }

  @Override
  public boolean isRegularFile() {
    try {
      return client.isRegularFile(path);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isDirectory() {
    try {
      return client.isDirectory(path);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isSymbolicLink() {
    return false;
  }

  @Override
  public boolean isOther() {
    return false;
  }

  @Override
  public long size() {
    return 0; // TODO
  }

  @Override
  public Object fileKey() {
    return path;
  }
}
