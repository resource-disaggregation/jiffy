package jiffy.storage;

import java.util.Optional;

public class BlockNameParser {

  public static class BlockMetadata {

    private String host;
    private int servicePort;
    private int managementPort;
    private int blockId;
    private Integer sequenceId;

    BlockMetadata(String host, int servicePort, int managementPort, int blockId) {
      this.host = host;
      this.servicePort = servicePort;
      this.managementPort = managementPort;
      this.blockId = blockId;
      this.sequenceId = null;
    }

    BlockMetadata(String host, int servicePort, int managementPort, int blockId, int sequenceId) {
      this.host = host;
      this.servicePort = servicePort;
      this.managementPort = managementPort;
      this.blockId = blockId;
      this.sequenceId = sequenceId;
    }

    public String getHost() {
      return host;
    }

    public int getServicePort() {
      return servicePort;
    }

    public int getManagementPort() {
      return managementPort;
    }

    public int getBlockId() {
      return blockId;
    }

    public Integer getSequenceId() {
      return sequenceId;
    }
  }

  public static BlockMetadata parse(String blockName) {
    System.out.println("blockName:" + blockName);
    String[] parts = blockName.split(":");
    if(parts.length == 5) {
      return new BlockMetadata(parts[0], Integer.parseInt(parts[1]), Integer.parseInt(parts[2]),
              Integer.parseInt(parts[3]), Integer.parseInt(parts[4]));
    }
    return new BlockMetadata(parts[0], Integer.parseInt(parts[1]), Integer.parseInt(parts[2]),
            Integer.parseInt(parts[3]));
  }

}
