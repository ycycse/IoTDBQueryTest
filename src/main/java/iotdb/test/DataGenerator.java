package iotdb.test;

import iotdb.test.Utils.Config;
import iotdb.test.Utils.DataUtils;
import iotdb.test.Utils.FileUtils;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.iotdb.jdbc.IoTDBStatement;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

public class DataGenerator {
  public static void addDataViaSession(String ip, int port, String devicePath) {
    try (Session session = new Session(ip, port, "root", "root")) {
      session.open(false);
      Config.setSessionIp(ip);
      Config.setPORT(port);
      DataUtils.addData(devicePath, 30, 100000);
    } catch (IoTDBConnectionException e) {
      e.printStackTrace();
    } catch (StatementExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  // weather data
  private static final String[] places =
      new String[] {
        "root.weather.London",
        "root.weather.Edinburgh",
        "root.weather.Belfast",
        "root.weather.Birmingham",
        "root.weather.Liverpool",
        "root.weather.Derby",
        "root.weather.Durham",
        "root.weather.Hereford",
        "root.weather.Manchester",
        "root.weather.Oxford"
      };

  public static void sortDataInsert(String ip, int port, int deviceId,int dataNum) throws SQLException, ClassNotFoundException {
    Class.forName(org.apache.iotdb.jdbc.Config.JDBC_DRIVER_NAME);

    int numOfDevice = deviceId;
    int numOfTSInDevice = dataNum;
    Random random = new Random();

    String deviceBaseName = "root.sg.d";
    long startTime = System.currentTimeMillis();

    try (Connection iotDBConnection =
                 DriverManager.getConnection("jdbc:iotdb://" + ip + ":" + port + "/", "root", "root");
         IoTDBStatement statement = (IoTDBStatement) iotDBConnection.createStatement()) {
        String deviceName = deviceBaseName + numOfDevice;
        createData(statement,deviceName);
        System.out.println("start to insert data in "+deviceName);
        for(int j=0;j<numOfTSInDevice;j++){

          String insertUniqueTime =
                  "INSERT INTO "
                          + deviceName
                          + "(timestamp,num,bigNum,floatNum,bool) VALUES("
                          + startTime
                          + ","
                          + random.nextInt()
                          + ","
                          + random.nextLong()
                          + ","
                          + random.nextDouble()
                          + ","
                          + random.nextBoolean()
                          + ")";

          statement.execute(insertUniqueTime);
          startTime += 10;
        }
        System.out.println("insert data" + numOfTSInDevice + "to" + deviceName);
    }
  }

  public static void sortDataInsert(String ip, int port, int deviceId,int dataNum,int columnNum) throws SQLException, ClassNotFoundException {
    Class.forName(org.apache.iotdb.jdbc.Config.JDBC_DRIVER_NAME);

    int numOfDevice = deviceId;
    int numOfTSInDevice = dataNum;
    Random random = new Random();

    String deviceBaseName = "root.sg2.d";
    long startTime = System.currentTimeMillis();

    try (Connection iotDBConnection =
                 DriverManager.getConnection("jdbc:iotdb://" + ip + ":" + port + "/", "root", "root");
         IoTDBStatement statement = (IoTDBStatement) iotDBConnection.createStatement()) {
      String deviceName = deviceBaseName + numOfDevice;
      createMoreData(statement,deviceName,columnNum);
      System.out.println("start to insert data in "+deviceName);
      for(int j=0;j<numOfTSInDevice;j++){
        String insertUniqueTime =
                "INSERT INTO "
                        + deviceName
                        + "(timestamp,";

        for(int k=0;k<columnNum;k++){
            insertUniqueTime+="num"+k+",";
            insertUniqueTime+="bigNum"+k+",";
            insertUniqueTime+="floatNum"+k+",";
            if(k!=columnNum - 1){
              insertUniqueTime+="bool"+k+",";
            }else {
              insertUniqueTime+="bool"+k+") VALUES("+startTime+",";
            }
        }

        for(int k=0;k<columnNum;k++){
            insertUniqueTime+=random.nextInt()+",";
            insertUniqueTime+=random.nextLong()+",";
            insertUniqueTime+=random.nextDouble()+",";
            if(k==columnNum - 1){
                insertUniqueTime+=random.nextBoolean()+")";
            }else {
                insertUniqueTime+=random.nextBoolean()+",";
            }
        }

//        System.out.println(insertUniqueTime);

        statement.execute(insertUniqueTime);
        startTime += 10;
      }
      System.out.println("insert data" + numOfTSInDevice + "to" + deviceName);
    }
  }


  private static void createData(IoTDBStatement statement,String deviceName) throws SQLException {
    String PRE_NUM = deviceName + ".num";
    String PRE_BIG_NUM = deviceName + ".bigNum";
    String PRE_FLOAT_NUM = deviceName + ".floatNum";
    String PRE_STR = deviceName + ".str";
    String PRE_BOOL = deviceName + ".bool";
    String createNumSql =
            "CREATE TIMESERIES " + PRE_NUM + " WITH DATATYPE=INT32, ENCODING=RLE";
    statement.execute(createNumSql);
    String createBigNumSql =
            "CREATE TIMESERIES " + PRE_BIG_NUM + " WITH DATATYPE=INT64, ENCODING=RLE";
    statement.execute(createBigNumSql);
    String createFloatNumSql =
            "CREATE TIMESERIES " + PRE_FLOAT_NUM + " WITH DATATYPE=DOUBLE, ENCODING=RLE";
    statement.execute(createFloatNumSql);
//    String createStrSql =
//            "CREATE TIMESERIES " + PRE_STR + " WITH DATATYPE=TEXT, ENCODING=PLAIN";
//    statement.execute(createStrSql);
    String createBoolSql =
            "CREATE TIMESERIES " + PRE_BOOL + " WITH DATATYPE=BOOLEAN, ENCODING=PLAIN";
    statement.execute(createBoolSql);
  }

  private static void createMoreData(IoTDBStatement statement,String deviceName, int i) throws SQLException {
    for(int j = 0;j<i;j++){
      String PRE_NUM = deviceName + ".num" + j;
      String PRE_BIG_NUM = deviceName + ".bigNum" + j;
      String PRE_FLOAT_NUM = deviceName + ".floatNum" + j;
      String PRE_STR = deviceName + ".str" + j;
      String PRE_BOOL = deviceName + ".bool" + j;
      String createNumSql =
              "CREATE TIMESERIES " + PRE_NUM + " WITH DATATYPE=INT32, ENCODING=RLE";
      statement.execute(createNumSql);
      String createBigNumSql =
              "CREATE TIMESERIES " + PRE_BIG_NUM + " WITH DATATYPE=INT64, ENCODING=RLE";
      statement.execute(createBigNumSql);
      String createFloatNumSql =
              "CREATE TIMESERIES " + PRE_FLOAT_NUM + " WITH DATATYPE=DOUBLE, ENCODING=RLE";
      statement.execute(createFloatNumSql);
//    String createStrSql =
//            "CREATE TIMESERIES " + PRE_STR + " WITH DATATYPE=TEXT, ENCODING=PLAIN";
//    statement.execute(createStrSql);
      String createBoolSql =
              "CREATE TIMESERIES " + PRE_BOOL + " WITH DATATYPE=BOOLEAN, ENCODING=PLAIN";
      statement.execute(createBoolSql);
    }
  }



  public static void crossTimestampWeatherData(String ip, int port)
      throws ClassNotFoundException, SQLException {
    Class.forName(org.apache.iotdb.jdbc.Config.JDBC_DRIVER_NAME);
    int numOfTSInDevice = 1000000;
    long startTime = 1668960890000L;
    long timeGap = 100L;
    long startPrecipitation = 200;
    double startTemperature = 20.0;
    Map<String, Double[]> deviceToMaxTemperature = new HashMap<>();
    Map<String, Double[]> deviceToAvgTemperature = new HashMap<>();
    Map<String, Long[]> deviceToMaxPrecipitation = new HashMap<>();
    Map<String, Double[]> deviceToAvgPrecipitation = new HashMap<>();

    try (Connection iotDBConnection =
            DriverManager.getConnection("jdbc:iotdb://" + ip + ":" + port + "/", "root", "root");
        IoTDBStatement statement = (IoTDBStatement) iotDBConnection.createStatement()) {
      // create TimeSeries
      for (String place : places) {
        String PRE_PRECIPITATION = place + ".precipitation";
        String PRE_TEMPERATURE = place + ".temperature";
        String createPrecipitationSql =
            "CREATE TIMESERIES " + PRE_PRECIPITATION + " WITH DATATYPE=INT64, ENCODING=RLE";
        String createTemperatureSql =
            "CREATE TIMESERIES " + PRE_TEMPERATURE + " WITH DATATYPE=DOUBLE, ENCODING=RLE";
        statement.execute(createPrecipitationSql);
        statement.execute(createTemperatureSql);
      }
      // insert data
      long start = startTime;
      double[][] temperatures = new double[places.length][29];
      long[][] precipitations = new long[places.length][29];
      for (int index = 0; index < places.length; index++) {
        String place = places[index];

        for (int i = 0; i < numOfTSInDevice; i++) {
          long precipitation = startPrecipitation + place.hashCode() + (start + i * timeGap);
          double temperature = startTemperature + place.hashCode() + (start + i * timeGap);
          precipitations[index][(int) ((start - startTime) / timeGap) + i] = precipitation;
          temperatures[index][(int) ((start - startTime) / timeGap) + i] = temperature;
          String insertUniqueTime =
              "INSERT INTO "
                  + place
                  + "(timestamp,precipitation,temperature) VALUES("
                  + (start + i * timeGap)
                  + ","
                  + precipitation
                  + ","
                  + temperature
                  + ")";
          statement.execute(insertUniqueTime);
        }
        start += timeGap;
      }

      for (int i = 0; i < places.length; i++) {
        Double[] aT = new Double[3];
        Double[] aP = new Double[3];
        Double[] mT = new Double[3];
        Long[] mP = new Long[3];
        double totalTemperature = 0;
        long totalPrecipitation = 0;
        double maxTemperature = -1;
        long maxPrecipitation = -1;
        int cnt = 0;
        for (int j = 0; j < precipitations[i].length; j++) {
          totalTemperature += temperatures[i][j];
          totalPrecipitation += precipitations[i][j];
          maxPrecipitation = Math.max(maxPrecipitation, precipitations[i][j]);
          maxTemperature = Math.max(maxTemperature, temperatures[i][j]);
          if ((j + 1) % 10 == 0 || j == precipitations[i].length - 1) {
            aT[cnt] = totalTemperature / 10;
            aP[cnt] = (double) totalPrecipitation / 10;
            mP[cnt] = maxPrecipitation;
            mT[cnt] = maxTemperature;
            cnt++;
            totalTemperature = 0;
            totalPrecipitation = 0;
            maxTemperature = -1;
            maxPrecipitation = -1;
          }
        }
        deviceToMaxTemperature.put(places[i], mT);
        deviceToMaxPrecipitation.put(places[i], mP);
        deviceToAvgTemperature.put(places[i], aT);
        deviceToAvgPrecipitation.put(places[i], aP);
      }
      FileUtils.writeMapToFile("./avgPrecipitation.txt", deviceToAvgPrecipitation);
      FileUtils.writeMapToFile("./avgTemperature.txt", deviceToAvgTemperature);
      FileUtils.writeMapToFile("./maxPrecipitation.txt", deviceToMaxPrecipitation);
      FileUtils.writeMapToFile("./maxTemperature.txt", deviceToMaxTemperature);
    }
  }

  public static void main(String[] args) {
    try {
//      sortDataInsert("127.0.0.1",6667,0,100000);
//      sortDataInsert("127.0.0.1",6667,1,300000,10);
//      sortDataInsert("127.0.0.1",6667,2,500000);
//      sortDataInsert("127.0.0.1",6667,3,700000,10);
//      sortDataInsert("127.0.0.1",6667,4,900000,10);
//      sortDataInsert("127.0.0.1",6667,5,1000000,10);
//      sortDataInsert("127.0.0.1",6667,6,2000000,10);
//      sortDataInsert("127.0.0.1",6667,7,4000000,10);
//      sortDataInsert("127.0.0.1",6667,8,6000000,10);
//      sortDataInsert("127.0.0.1",6667,9,8000000,10);
//      sortDataInsert("127.0.0.1",6667,10,10000000,10);
//      sortDataInsert("127.0.0.1",6667,11,20000000,10);
//      sortDataInsert("127.0.0.1",6667,12,30000000,10);
//      sortDataInsert("127.0.0.1",6667,13,40000000,10);
//      sortDataInsert("127.0.0.1",6667,14,50000000,10);

      sortDataInsert("127.0.0.1",6667,11,110000);
      sortDataInsert("127.0.0.1",6667,12,120000);
      sortDataInsert("127.0.0.1",6667,13,130000);
      sortDataInsert("127.0.0.1",6667,14,140000);
      sortDataInsert("127.0.0.1",6667,15,150000);
      sortDataInsert("127.0.0.1",6667,16,160000);
      sortDataInsert("127.0.0.1",6667,17,170000);
      sortDataInsert("127.0.0.1",6667,18,180000);
      sortDataInsert("127.0.0.1",6667,19,190000);
      sortDataInsert("127.0.0.1",6667,20,200000);
//      sortDataInsert("127.0.0.1",6667,6,2000000);
//      sortDataInsert("127.0.0.1",6667,7,4000000);
//      sortDataInsert("127.0.0.1",6667,8,6000000);
//      sortDataInsert("127.0.0.1",6667,9,8000000);
//      sortDataInsert("127.0.0.1",6667,10,10000000);
//      sortDataInsert("127.0.0.1",6667,11,20000000);
//      sortDataInsert("127.0.0.1",6667,12,30000000);
//      sortDataInsert("127.0.0.1",6667,13,40000000);
//      sortDataInsert("127.0.0.1",6667,14,50000000);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
