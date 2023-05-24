package iotdb.test.Utils;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Map;

public class FileUtils {

  private static final String lineSeparator = System.getProperty("line.separator");

  public static <K, V> void writeMapToFile(String fileName, Map<K, V> map) {
    try (FileWriter fw = new FileWriter(fileName, true)) {
      fw.write(
          "Write a map "
              + map.getClass().getName()
              + " in "
              + new Timestamp(System.currentTimeMillis())
              + lineSeparator);
      StringBuilder input = new StringBuilder();
      for (K key : map.keySet()) {
        V value = map.get(key);
        input
            .append(key)
            .append(":")
            .append(value)
            .append(lineSeparator);
      }
      fw.write(String.valueOf(input));
      fw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static <K, V> void writeMapToFile(Map<K, V> map) {
    String fileName = "./" + map.getClass().getSimpleName() + ".txt";
    writeMapToFile(fileName, map);
  }

  public static void writeLongToFile(long value, String fileName) {
    try (FileWriter fw = new FileWriter(fileName, true)) {
      fw.write(String.valueOf(value));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void writeDoubleToFile(double value, String fileName) {
    try (FileWriter fw = new FileWriter(fileName, true)) {
      fw.write(String.valueOf(value));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
