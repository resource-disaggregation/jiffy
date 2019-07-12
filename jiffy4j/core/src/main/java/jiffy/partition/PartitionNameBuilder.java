package jiffy.partition;

public abstract class PartitionNameBuilder {
  int numPartitions;

  PartitionNameBuilder(int numPartitions) {
    this.numPartitions = numPartitions;
  }

  public abstract String partitionName(int partitionId);

  public static PartitionNameBuilder get(String type, int numPartitions) {
    if ("hashtable".equals(type)) {
      return new HashPartitionNameBuilder(numPartitions);
    }
    return new DefaultPartitionNameBuilder(numPartitions);
  }
}
