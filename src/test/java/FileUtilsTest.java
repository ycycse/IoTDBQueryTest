import iotdb.test.Utils.FileUtils;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class FileUtilsTest {
  @Test
  public void testWriteMapToFile() {
    Map<String, Double[]> map = new HashMap<>();
    map.put("test1", new Double[] {123.4534, 1321.231});
    map.put("test2", new Double[] {123.213, 1231.231});
    FileUtils.writeMapToFile("./test.txt", map);
  }
}
