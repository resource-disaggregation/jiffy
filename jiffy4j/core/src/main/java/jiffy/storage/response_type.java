/**
 * Autogenerated by Thrift Compiler (0.12.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package jiffy.storage;


@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.12.0)")
public enum response_type implements org.apache.thrift.TEnum {
  subscribe(0),
  unsubscribe(1);

  private final int value;

  private response_type(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  @org.apache.thrift.annotation.Nullable
  public static response_type findByValue(int value) { 
    switch (value) {
      case 0:
        return subscribe;
      case 1:
        return unsubscribe;
      default:
        return null;
    }
  }
}
