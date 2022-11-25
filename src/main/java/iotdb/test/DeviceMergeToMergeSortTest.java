package iotdb.test;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;

public class DeviceMergeToMergeSortTest {

    static String sql = "SELECT * FROM root.weather.** align by device";
    public static void main(String[] args) {
        try (Session session = new Session("127.0.0.1", 6667, "root", "root")) {
            long ret = 0;
            session.open(false);
//            DataUtils.addMultiData(20,100,10000);
            for(int i=0;i<10;i++){
                SessionDataSet sessionDataSet = session.executeQueryStatement(sql, 50000);
                while (sessionDataSet.hasNext()) sessionDataSet.next();
            }
            for(int i=0;i<10;i++){
                long start = System.nanoTime();
                SessionDataSet sessionDataSet = session.executeQueryStatement(sql, 50000);
                while (sessionDataSet.hasNext()) sessionDataSet.next();
                long end = System.nanoTime();
                ret += end;
//                System.out.println(end-start);
            }
            System.out.println(ret);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            e.printStackTrace();
        }
    }
}
