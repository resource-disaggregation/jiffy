package jiffy.partition;

public class DefaultPartitionNameBuilder extends PartitionNameBuilder {

  DefaultPartitionNameBuilder(int numPartitions) {
    super(numPartitions);
  }

  @Override
  public String partitionName(int partitionId) {
    return String.valueOf(partitionId);
  }
}
