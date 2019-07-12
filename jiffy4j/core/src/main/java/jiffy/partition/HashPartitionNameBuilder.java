package jiffy.partition;

import static jiffy.storage.HashSlot.SLOT_MAX;

public class HashPartitionNameBuilder extends PartitionNameBuilder {

  private int hashRange;

  public HashPartitionNameBuilder(int numPartitions) {
    super(numPartitions);
    hashRange = SLOT_MAX / numPartitions;
  }

  @Override
  public String partitionName(int i) {
    int begin = i * hashRange;
    int end = (i == numPartitions - 1) ? SLOT_MAX : (i + 1) * hashRange;
    return begin + "_" + end;
  }
}
