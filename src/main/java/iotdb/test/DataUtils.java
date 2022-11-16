package iotdb.test;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DataUtils {
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
    static int[] dataNums = new int[]{
            1,
            1000,
            10000,
            100000,
            200000,
            300000,
            400000,
            500000,
            600000,
            700000,
            800000,
            900000,
            1000000
    };
    public static void addMultiData(int deviceNum, int timeseriesNum, int dataNum)
        throws IoTDBConnectionException, StatementExecutionException {
        Session anotherSession = new Session(Config.SESSION_IP, Config.PORT, Config.USERNAME, Config.PASSWORD);
        anotherSession.open();
        Random random = new Random();

        deviceNum = dataNums.length;
//        String device_id = Config.DEVICE_ID + (char)(random.nextInt(2) % 2 == 0 ? 65 : 97+random.nextInt(26))+(char)(random.nextInt(2) % 2 == 0 ? 65 : 97+random.nextInt(26))+(char)(random.nextInt(2) % 2 == 0 ? 65 : 97+random.nextInt(26))+(char)(random.nextInt(2) % 2 == 0 ? 65 : 97+random.nextInt(26))+(char)(random.nextInt(2) % 2 == 0 ? 65 : 97+random.nextInt(26))+(char)(random.nextInt(2) % 2 == 0 ? 65 : 97+random.nextInt(26))+(char)(random.nextInt(2) % 2 == 0 ? 65 : 97+random.nextInt(26))+(char)(random.nextInt(2) % 2 == 0 ? 65 : 97+random.nextInt(26))+(char)(random.nextInt(2) % 2 == 0 ? 65 : 97+random.nextInt(26))+(char)(random.nextInt(2) % 2 == 0 ? 65 : 97+random.nextInt(26));
        for(int i=0;i<deviceNum;i++){
            long timestamp = System.currentTimeMillis();
//            String device_id = Config.DEVICE_ID + place[i];
            String device_id = "root.deserializationTest.thresholdTest.d"+"_"+i;
            List<MeasurementSchema> schemaList = new ArrayList<>();
            for(int j=0;j<timeseriesNum;j++){
                schemaList.add(new MeasurementSchema("s_"+j, TSDataType.INT32));
//                schemaList.add(new MeasurementSchema("temperature", TSDataType.DOUBLE));
            }
            Tablet tablet = new Tablet(device_id, schemaList, 500);

            for (int row = 0; row < dataNums[i]; row++) {
                int r = tablet.rowSize++;

                tablet.addTimestamp(r, timestamp);
                for(int k=0;k<timeseriesNum;k++){
                    tablet.addValue(schemaList.get(k).getMeasurementId(), r, getValue(Type.INT32));
//                    tablet.addValue(schemaList.get(1+2*k).getMeasurementId(), r, getValue(Type.DOUBLE));
                }


                if (tablet.rowSize == tablet.getMaxRowNumber()) {
                    anotherSession.insertTablet(tablet);
                    tablet.reset();
                }
                timestamp++;
            }

            if (tablet.rowSize != 0) {
                anotherSession.insertTablet(tablet);
                tablet.reset();
            }

        }
        anotherSession.close();;
    }
    static Object getValue(Type type) {
        switch (type) {
            case INT64:
                return new Random().nextLong();
            case INT32:
                return new Random().nextInt(800);
            case FLOAT:
                return new Random().nextFloat();
            case DOUBLE:
                return queryDouble();
            case BOOLEAN:
                return new Random().nextBoolean();
            case TEXT:
                return RandomStringUtils.random(10);
            default:
                return null;
        }
    }

    public static Double queryDouble() {
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
