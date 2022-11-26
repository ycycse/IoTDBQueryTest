package iotdb.test;

import iotdb.test.Utils.Config;
import iotdb.test.Utils.DataUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;

public class DataGenerator {
    public static void addDataViaSession(String ip,int port,String devicePath){
        try (Session session = new Session(ip, port, "root", "root")) {
            session.open(false);
            Config.setSessionIp(ip);
            Config.setPORT(port);
            DataUtils.addData(devicePath,30,500);
        } catch (IoTDBConnectionException e) {
            e.printStackTrace();
        } catch (StatementExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        addDataViaSession("127.0.0.1",6667,"root.test2.sg");
        addDataViaSession("127.0.0.1",6667,"root.test2.sg2");
        addDataViaSession("127.0.0.1",6669,"root.test2.sg3");
        addDataViaSession("127.0.0.1",6671,"root.test2.sg4");
    }
}
