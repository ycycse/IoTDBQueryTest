package iotdb.test;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.OldSessionDataSet;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;

public class DeviceMergeToMergeSortTest {

    static String sql = "SELECT * FROM root.weather.** where time<2022-11-21T12:09:22.666+08:00 align by device";
    public static void main(String[] args) {
        try (Session session = new Session("127.0.0.1", 6667, "root", "root")) {

            session.open(false);
//            DataUtils.addMultiData(20,100,10);
            for(int i=0;i<5;i++){
                SessionDataSet sessionDataSet = session.executeQueryStatement(sql, 50000);
                while (sessionDataSet.hasNext()) sessionDataSet.next();
            }
            for(int i=0;i<10;i++){
                long start = System.nanoTime();
                SessionDataSet sessionDataSet = session.executeQueryStatement(sql, 50000);
                long end = System.nanoTime();
                while (sessionDataSet.hasNext()) sessionDataSet.next();
                System.out.println(end-start);
            }

        } catch (IoTDBConnectionException | StatementExecutionException e) {
            e.printStackTrace();
        }
    }
}
