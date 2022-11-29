package iotdb.test;

import iotdb.test.Utils.Config;
import java.sql.SQLException;
import java.util.Random;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

public class PerformanceTest {
  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException, SQLException,
          ClassNotFoundException, InterruptedException {
    //        Thread.sleep(50000);
    int deviceNum = Integer.parseInt(args[0]);
    int timeSeriesNum = Integer.parseInt(args[1]);
    int dataNum = Integer.parseInt(args[2]);

    //        DataUtils.addMultiData(deviceNum,timeSeriesNum,dataNum);
    System.exit(0);
    Random random = new Random();
    String devices = "";
    int startIndex = 0;
    int endIndex = startIndex + 1;
    for (int i = startIndex; i < endIndex; i++) {
      if (i == endIndex - 1) devices += Config.DEVICE_ID + random.nextInt(deviceNum);
      else devices += Config.DEVICE_ID + random.nextInt(deviceNum) + ",";
    }

    String sql = "select * from " + devices;
    System.out.println("sql : " + sql);
    System.out.println("--------------------new Session--------------------");
    for (int i = 0; i < 10; i++) {
      System.out.println(i + " : " + PerformanceTest2.SessionTest1(sql));
    }
    System.out.println("--------------------old Session--------------------");
    for (int i = 0; i < 10; i++) {
      System.out.println(i + " : " + PerformanceTest2.SessionTest2(sql));
    }
    Class.forName(org.apache.iotdb.jdbc.Config.JDBC_DRIVER_NAME);
    System.out.println("--------------------new JDBC--------------------");
    for (int i = 0; i < 10; i++) {
      System.out.println(i + " : " + PerformanceTest2.JDBCTest1(sql));
    }
    System.out.println("--------------------old JDBC--------------------");
    for (int i = 0; i < 10; i++) {
      System.out.println(i + " : " + PerformanceTest2.JDBCTest2(sql));
    }
  }
}
