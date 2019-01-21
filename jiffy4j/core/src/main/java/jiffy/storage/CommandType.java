package jiffy.storage;

public enum CommandType {
  accessor, mutator;

  private static final CommandType[] opTypes = {CommandType.accessor,
      CommandType.accessor,
      CommandType.accessor,
      CommandType.accessor,
      CommandType.mutator,
      CommandType.mutator,
      CommandType.mutator,
      CommandType.mutator,
      CommandType.mutator,
      CommandType.accessor,
      CommandType.accessor,
      CommandType.accessor,
      CommandType.accessor,
      CommandType.accessor,
      CommandType.mutator,
      CommandType.mutator,
  };

  public static CommandType opType(int op) {
    return opTypes[op];
  }
}
