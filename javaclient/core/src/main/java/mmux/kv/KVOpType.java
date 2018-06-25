package mmux.kv;

public enum KVOpType {
  accessor, mutator;

  private static final KVOpType[] opTypes = {accessor, accessor, accessor, accessor, mutator,
      mutator, mutator};

  public static KVOpType opType(int op) {
    return opTypes[op];
  }
}
