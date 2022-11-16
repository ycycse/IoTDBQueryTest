package iotdb.test;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.OldSessionDataSet;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;

import java.sql.SQLOutput;

public class DeserializationTest {

    static String device = "root.deserializationTest.node";
//    static String sql = "select s_1 from root.deserializationTest.node";
    static String sql = "select * from root.performance.test0";

    public static void main(String[] args) throws InterruptedException {
        Thread.sleep(10000);
        try(Session session = new Session(Config.SESSION_IP, Config.PORT, Config.USERNAME, Config.PASSWORD)) {
            session.open(false);
            OldSessionDataSet oldSessionDataSet =
                    session.OldexecuteQueryStatement(sql,1000);
            long oldStart = System.nanoTime();
            while(oldSessionDataSet.hasNext()){
                oldSessionDataSet.next();
            }
            long oldEnd = System.nanoTime();


            SessionDataSet sessionDataSet =
                    session.executeQueryStatement(sql,1000);
            long newStart = System.nanoTime();
            while(sessionDataSet.hasNext()){
                sessionDataSet.next();
            }
            long newEnd = System.nanoTime();
            System.out.println("--------new V1.0--------");
            System.out.println(newEnd-newStart);
            System.out.println("--------old 13.0--------");
            System.out.println(oldEnd-oldStart);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            e.printStackTrace();
        };
    }

}
