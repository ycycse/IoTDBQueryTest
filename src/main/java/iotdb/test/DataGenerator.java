package iotdb.test;

import iotdb.test.Utils.Config;
import iotdb.test.Utils.DataUtils;
import iotdb.test.Utils.FileUtils;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
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
      DataUtils.addData(devicePath, 30, 500);
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

  public static void crossTimestampWeatherData(String ip, int port)
      throws ClassNotFoundException, SQLException {
    Class.forName(org.apache.iotdb.jdbc.Config.JDBC_DRIVER_NAME);
    int numOfTSInDevice = 20;
    long startTime = 1668960000000L;
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
      crossTimestampWeatherData("127.0.0.1", 6667);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
