package mmux.kv;

public enum KVOpType {
  accessor, mutator;

  private static final KVOpType[] opTypes = {KVOpType.accessor,
      KVOpType.accessor,
      KVOpType.accessor,
      KVOpType.accessor,
      KVOpType.mutator,
      KVOpType.mutator,
      KVOpType.mutator,
      KVOpType.mutator,
      KVOpType.mutator,
      KVOpType.accessor,
      KVOpType.accessor,
      KVOpType.accessor,
      KVOpType.accessor,
      KVOpType.accessor};

  public static KVOpType opType(int op) {
    return opTypes[op];
  }
}
