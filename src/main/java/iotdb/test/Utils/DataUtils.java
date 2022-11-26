package iotdb.test.Utils;

import iotdb.test.Utils.Config;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DataUtils {
    static Logger logger = LoggerFactory.getLogger(DataUtils.class.getName());
    enum Type {
        INT32,
        INT64,
        FLOAT,
        DOUBLE,
        BOOLEAN,
        TEXT
    }
    static String[] place = new String[]{
      "London",
      "Edinburgh",
      "Belfast",
      "Birmingham",
      "Liverpool",
      "Derby",
      "Durham",
      "Hereford",
      "Manchester",
      "Oxford",
      "Plymouth",
      "Portsmouth",
      "Preston"
    };
    private static void insertData(Session session,String devicePath,int sensorNum,int dataNum) throws IoTDBConnectionException, StatementExecutionException {
        logger.info("inserting data to "+devicePath+" in "+Config.IP+":"+Config.PORT);
        long timestamp = 1668960000L;
        int num = 0;
        List<MeasurementSchema> schemaList = new ArrayList<>();
        for(int j=0;j<sensorNum;j++){
            schemaList.add(new MeasurementSchema("s_"+j, TSDataType.INT32));
        }
        Tablet tablet = new Tablet(devicePath, schemaList, 400);

        for (int row = 0; row < dataNum; row++) {
            int r = tablet.rowSize++;

            tablet.addTimestamp(r, timestamp);
            for(int k=0;k<sensorNum;k++){
                tablet.addValue(schemaList.get(k).getMeasurementId(), r, num);
                num++;
            }


            if (tablet.rowSize == tablet.getMaxRowNumber()) {
                session.insertTablet(tablet);
                tablet.reset();
                session.executeNonQueryStatement("flush");
            }
            timestamp++;
        }

        if (tablet.rowSize != 0) {
            session.insertTablet(tablet);
            tablet.reset();
            session.executeNonQueryStatement("flush");
        }
    }
    public static void addData(String devicePath,int sensorNum,int dataNum) throws IoTDBConnectionException, StatementExecutionException {
        Session anotherSession = new Session(Config.SESSION_IP, Config.PORT, Config.USERNAME, Config.PASSWORD);
        anotherSession.open();
        insertData(anotherSession,devicePath,sensorNum,dataNum);
        anotherSession.close();
    }
    public static void addMultiData(String[] devicePaths, int sensorNum, int dataNum)
        throws IoTDBConnectionException, StatementExecutionException {
        Session anotherSession = new Session(Config.SESSION_IP, Config.PORT, Config.USERNAME, Config.PASSWORD);
        anotherSession.open();
        for(int i=0;i<devicePaths.length;i++){
            String device_id = devicePaths[i];
            insertData(anotherSession,device_id,sensorNum,dataNum);
        }
        anotherSession.close();
    }
    private static Object getValue(Type type) {
        switch (type) {
            case INT64:
                return new Random().nextLong();
            case INT32:
                return new Random().nextInt(800);
            case FLOAT:
                return new Random().nextFloat();
            case DOUBLE:
                return getDouble();
            case BOOLEAN:
                return new Random().nextBoolean();
            case TEXT:
                return RandomStringUtils.random(10);
            default:
                return null;
        }
    }

    private static Double getDouble() {
        Random rand = new Random();
        double MAX=26.5;
        double MIN=38.4;
        double result=0;
        for(int i=0; i<10; i++){
            result = MIN + (rand.nextDouble() * (MAX - MIN));
            result = (double) Math.round(result * 10) / 10;
        }
        return result;
    }

}
