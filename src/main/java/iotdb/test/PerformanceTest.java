package iotdb.test;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.iotdb.jdbc.IoTDBStatement;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class PerformanceTest {

//    public static void addMultiData(int deviceNum, int timeseriesNum, int dataNum, String id)
//            throws IoTDBConnectionException, StatementExecutionException {
//        Session anotherSession = new Session(id, 6667, USERNAME, PASSWORD);
//        anotherSession.open();
//
//        for(int i=0;i<deviceNum;i++){
//            String device_id = DEVICE_ID + i;
//            List<MeasurementSchema> schemaList = new ArrayList<>();
//            for(int j=0;j<timeseriesNum;j++){
//                schemaList.add(new MeasurementSchema("testLong"+j, TSDataType.INT64));
//                schemaList.add(new MeasurementSchema("testDouble"+j, TSDataType.DOUBLE));
//            }
//            Tablet tablet = new Tablet(device_id, schemaList, 1000);
//
//            long timestamp = System.currentTimeMillis();
//            for (int row = 0; row < dataNum; row++) {
//                int r = tablet.rowSize++;
//
//                tablet.addTimestamp(r, timestamp);
//                for(int k=0;k<timeseriesNum;k++){
//                    tablet.addValue(schemaList.get(2 * k).getMeasurementId(), r, getValue(Type.INT64));
//                    tablet.addValue(schemaList.get(1+2*k).getMeasurementId(), r, getValue(Type.DOUBLE));
//                }
//
//
//                if (tablet.rowSize == tablet.getMaxRowNumber()) {
//                    anotherSession.insertTablet(tablet);
//                    tablet.reset();
//                }
//                timestamp++;
//            }
//
//            if (tablet.rowSize != 0) {
//                anotherSession.insertTablet(tablet);
//                tablet.reset();
//            }
//
//        }
//        anotherSession.close();;
//    }

//    public static void addAlignedMultiData(int deviceNum, int timeseriesNum, int dataNum, String id)
//            throws IoTDBConnectionException, StatementExecutionException {
//        Session anotherSession = new Session(id, 6667, USERNAME, PASSWORD);
//        anotherSession.open();
//
//        for(int i=0;i<deviceNum;i++){
//            String device_id = DEVICE_ID + i;
//            List<MeasurementSchema> schemaList = new ArrayList<>();
//            for(int j=0;j<timeseriesNum;j++){
//                schemaList.add(new MeasurementSchema("testLong"+j, TSDataType.INT64));
//                schemaList.add(new MeasurementSchema("testDouble"+j, TSDataType.DOUBLE));
//            }
//            Tablet tablet = new Tablet(device_id, schemaList, 1000);
//
//            long timestamp = System.currentTimeMillis();
//            for (int row = 0; row < dataNum; row++) {
//                int r = tablet.rowSize++;
//
//                tablet.addTimestamp(r, timestamp);
//                for(int k=0;k<timeseriesNum;k++){
//                    tablet.addValue(schemaList.get(2 * k).getMeasurementId(), r, getValue(Type.INT64));
//                    tablet.addValue(schemaList.get(1+2*k).getMeasurementId(), r, getValue(Type.DOUBLE));
//                }
//
//
//                if (tablet.rowSize == tablet.getMaxRowNumber()) {
//                    anotherSession.insertTablet(tablet);
//                    tablet.reset();
//                }
//                timestamp++;
//            }
//
//            if (tablet.rowSize != 0) {
//                anotherSession.insertAlignedTablet(tablet);
//                tablet.reset();
//            }
//
//        }
//        anotherSession.close();;
//    }


//    /**
//     * randomly insert the same type of timeSeries
//     *
//     * @param type the type of timeSeries
//     * @param timeSeriesNum the number of timeSeries
//     * @param dataNum the row of data to be inserted
//     */
//    public static void addData(Type type, int timeSeriesNum, int dataNum)
//            throws IoTDBConnectionException, StatementExecutionException {
//        List<MeasurementSchema> schemaList = new ArrayList<>();
//        Session anotherSession = new Session(IP, 6669, USERNAME, PASSWORD);
//        anotherSession.open();
//        for (int i = 0; i < timeSeriesNum; i++) {
//            switch (type) {
//                case INT32:
//                    schemaList.add(new MeasurementSchema("testInt" + i, TSDataType.INT32));
//                    break;
//                case INT64:
//                    schemaList.add(new MeasurementSchema("testLong" + i, TSDataType.INT64));
//                    break;
//                case FLOAT:
//                    schemaList.add(new MeasurementSchema("testFloat" + i, TSDataType.FLOAT));
//                    break;
//                case DOUBLE:
//                    schemaList.add(new MeasurementSchema("testDouble" + i, TSDataType.DOUBLE));
//                    break;
//                case BOOLEAN:
//                    schemaList.add(new MeasurementSchema("testBoolean" + i, TSDataType.BOOLEAN));
//                    break;
//                case TEXT:
//                    schemaList.add(new MeasurementSchema("testText" + i, TSDataType.TEXT));
//            }
//        }
//
//        Tablet tablet = new Tablet(DEVICE_ID, schemaList, 100);
//        long timestamp = System.currentTimeMillis();
//        for (int row = 0; row < dataNum; row++) {
//            int r = tablet.rowSize++;
//            tablet.addTimestamp(r, timestamp);
//            for (int i = 0; i < timeSeriesNum; i++) {
//                tablet.addValue(schemaList.get(i).getMeasurementId(), r, getValue(type));
//            }
//            if (tablet.rowSize == tablet.getMaxRowNumber()) {
//                anotherSession.insertTablet(tablet);
//                tablet.reset();
//            }
//            timestamp++;
//        }
//        if (tablet.rowSize != 0) {
//            anotherSession.insertTablet(tablet);
//            tablet.reset();
//        }
//        anotherSession.close();
//    }
    public static void main(String[] args)
            throws IoTDBConnectionException, StatementExecutionException, SQLException, ClassNotFoundException, InterruptedException {
//        Thread.sleep(50000);
        int deviceNum = Integer.parseInt(args[0]);
        int timeSeriesNum = Integer.parseInt(args[1]);
        int dataNum = Integer.parseInt(args[2]);

        DataUtils.addMultiData(deviceNum,timeSeriesNum,dataNum);
        System.exit(0);
        Random random = new Random();
        String devices = "";
        int startIndex = 0;
        int endIndex = startIndex + 1;
        for(int i=startIndex;i<endIndex;i++){
            if(i==endIndex-1)devices += Config.DEVICE_ID+random.nextInt(deviceNum);
            else devices += Config.DEVICE_ID+random.nextInt(deviceNum)+",";
        }

        String sql = "select * from " + devices;
        System.out.println("sql : " + sql);
        System.out.println("--------------------new Session--------------------");
        for(int i=0;i<10;i++){
            System.out.println(i+" : "+PerformanceTest2.SessionTest1(sql));
        }
        System.out.println("--------------------old Session--------------------");
        for(int i=0;i<10;i++){
            System.out.println(i+" : "+PerformanceTest2.SessionTest2(sql));
        }
        Class.forName(org.apache.iotdb.jdbc.Config.JDBC_DRIVER_NAME);
        System.out.println("--------------------new JDBC--------------------");
        for(int i=0;i<10;i++){
            System.out.println(i+" : "+PerformanceTest2.JDBCTest1(sql));
        }
        System.out.println("--------------------old JDBC--------------------");
        for(int i=0;i<10;i++){
            System.out.println(i+" : "+PerformanceTest2.JDBCTest2(sql));
        }
    }
}
