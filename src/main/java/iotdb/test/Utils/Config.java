package iotdb.test.Utils;

public class Config {
  public static String USERNAME = "root";
  public static String PASSWORD = "root";
  public static int PORT = 6667;
  public static String IP = "192.168.130.14";
  public static String SESSION_IP = "127.0.0.1";
  public static String JDBC_IP = "192.168.130.13";
  public static String STORAGE_GROUP = "root.weather";
  public static String DEVICE_ID = STORAGE_GROUP + ".";

  public static void setSessionIp(String sessionIp) {
    SESSION_IP = sessionIp;
  }

  public static void setPORT(int PORT) {
    Config.PORT = PORT;
  }
}
