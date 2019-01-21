package jiffy.directory;

public class Permissions {
  public static final int none = 0;

  public static final int owner_read = 0400;
  public static final int owner_write = 0200;
  public static final int owner_exec = 0100;
  public static final int owner_all = 0700;

  public static final int group_read = 040;
  public static final int group_write = 020;
  public static final int group_exec = 010;
  public static final int group_all = 070;

  public static final int others_read = 04;
  public static final int others_write = 02;
  public static final int others_exec = 01;
  public static final int others_all = 07;

  public static final int all = 0777;

  public static final int set_uid = 04000;
  public static final int set_gid = 02000;
  public static final int sticky_bit = 01000;

  public static final int mask = 07777;
}
