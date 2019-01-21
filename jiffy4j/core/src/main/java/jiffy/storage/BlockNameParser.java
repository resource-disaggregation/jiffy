package jiffy.storage;

public class BlockNameParser {

  public static class BlockMetadata {

    private String host;
    private int servicePort;
    private int managementPort;
    private int notificationPort;
    private int chainPort;
    private int blockId;

    BlockMetadata(String host, int servicePort, int managementPort, int notificationPort,
        int chainPort, int blockId) {
      this.host = host;
      this.servicePort = servicePort;
      this.managementPort = managementPort;
      this.notificationPort = notificationPort;
      this.chainPort = chainPort;
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

    public int getNotificationPort() {
      return notificationPort;
    }

    public int getChainPort() {
      return chainPort;
    }

    public int getBlockId() {
      return blockId;
    }
  }

  public static BlockMetadata parse(String blockName) {
    String[] parts = blockName.split(":");
    return new BlockMetadata(parts[0], Integer.parseInt(parts[1]), Integer.parseInt(parts[2]),
        Integer.parseInt(parts[3]), Integer.parseInt(parts[4]), Integer.parseInt(parts[5]));
  }

}
