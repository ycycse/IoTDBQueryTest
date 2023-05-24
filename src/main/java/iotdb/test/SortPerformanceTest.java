/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package iotdb.test;

import iotdb.test.Utils.Config;
import iotdb.test.Utils.FileUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class SortPerformanceTest {

    static double sortTest(String sql) {
        try (Session session =
                     new Session(Config.SESSION_IP, Config.PORT, Config.USERNAME, Config.PASSWORD)) {
            session.open(false);
            long count = 0;
            long newStart = System.nanoTime();
            SessionDataSet sessionDataSet = session.executeQueryStatement(sql, 1000000);
            SessionDataSet.DataIterator dataIterator = sessionDataSet.iterator();
            int columnCount = sessionDataSet.getColumnNames().size();
            while (dataIterator.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    dataIterator.getString(i);
                }
                count++;
            }
            long newEnd = System.nanoTime();
            System.out.println("processed count: " + count);
            return (double) (newEnd - newStart) / 1e6;
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public static void correctnessCheck(String sql){
        try (Session session =
                     new Session(Config.SESSION_IP, Config.PORT, Config.USERNAME, Config.PASSWORD)) {
            session.open(false);
            long count = 0;
            SessionDataSet sessionDataSet = session.executeQueryStatement(sql, 1000000);
            SessionDataSet.DataIterator dataIterator = sessionDataSet.iterator();
            int lastValue = -1;
            boolean hasLastValue = false;
            while (dataIterator.next()) {
                long time = dataIterator.getLong(1);
                int num = dataIterator.getInt(2);
                if(hasLastValue){
                    if(num<lastValue){
                        throw new RuntimeException("wrong order");
                    }
                    lastValue = num;
                }else {
                    lastValue = num;
                    hasLastValue = true;
                }
                count++;
            }
            System.out.println("processed count: " + count);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            e.printStackTrace();
        }
    }

    public static void warmUp(String sql){
        long totoal = 0;
        for(int i=0;i<10;i++){
            totoal += sortTest(sql);
        }
        System.out.println("warm up:"+totoal/10);
    }

    private static final String orderByClause = " order by num";
    private static final String baseSQL = "select * from root.sg.d";
    private static final String baseSQL2 = "select * from root.sg2.d";
    private static final String savedPath = "./sortResult/";

    public static void main(String[] args) throws IOException {
//        correctnessCheck("select num,bigNum,floatNum from root.sg.d9 order by num");
//        if(true){
//            return;
//        }
        int startIndex = 0;
        int endIndex = 14;
        boolean res = Paths.get(savedPath).toFile().mkdirs();
        if(!res){
            System.out.println("mkdir failed");
        }
//        Map<String,String> ans = new HashMap<>();
//
//        for(int i=startIndex;i<=endIndex;i++){
//            String sql = baseSQL + i + orderByClause;
//            warmUp(sql);
//            double total = 0;
//            for(int j=0;j<5;j++){
//                total += sortTest(sql);
//            }
//            double result = total/5;
//            System.out.println(i+":"+result);
//            ans.put(sql," "+result+" ");
//        }
//        System.out.println(ans);
//        String fileName =savedPath  + "result.txt";
//        FileUtils.writeMapToFile(fileName,ans);
//
        Map<String,String> ans2 = new HashMap<>();

        for(int i=startIndex;i<=endIndex;i++){
            String sql = baseSQL2 + i + " order by num0";
            warmUp(sql);
            double total = 0;
            for(int j=0;j<5;j++){
                total += sortTest(sql);
            }
            double result = total/5;
            System.out.println(i+":"+result);
            ans2.put(sql," "+result+" ");
        }
        System.out.println(ans2);
        String fileName2 =savedPath  + "result2.txt";
        FileUtils.writeMapToFile(fileName2,ans2);
    }
}
