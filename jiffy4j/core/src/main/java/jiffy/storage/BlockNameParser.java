package jiffy.storage;

public class BlockNameParser {

  public static class BlockMetadata {

    private String host;
    private int servicePort;
    private int managementPort;
    private int blockId;

    BlockMetadata(String host, int servicePort, int managementPort, int blockId) {
      this.host = host;
      this.servicePort = servicePort;
      this.managementPort = managementPort;
      this.blockId = blockId;
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
  }

  public static BlockMetadata parse(String blockName) {
    String[] parts = blockName.split(":");
    return new BlockMetadata(parts[0], Integer.parseInt(parts[1]), Integer.parseInt(parts[2]),
            Integer.parseInt(parts[3]));
  }

}
