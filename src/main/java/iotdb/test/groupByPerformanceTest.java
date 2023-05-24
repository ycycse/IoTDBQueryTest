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
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;

import java.util.ArrayList;

public class groupByPerformanceTest {
    private static double test(String sql){
        try (
                Session session =
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
    public static void main(String[] args) {
        String device = "root.sg.d";
        ArrayList<Double> rawRes =  new ArrayList<>();
        ArrayList<Double> aggregationRes = new ArrayList<>();
        for(int i=1;i<=20;i++){
            String curDevice = device + i;
            String sql = "select count(floatNum) from "+ curDevice;
            String groupBySQl = sql + " group by count(floatNum,10)";
//            test(sql);test(groupBySQl);           test(sql);test(groupBySQl);           test(sql);test(groupBySQl);           test(sql);test(groupBySQl);           test(sql);test(groupBySQl);


//            System.out.println("sql: " + sql);
//            System.out.println("time: " + test(sql));
            rawRes.add(test(sql));
//            aggregationRes.add(test(groupBySQl));
//            System.out.println("sql: " + groupBySQl);
//            System.out.println("time: " + test(groupBySQl));
        }
        System.out.println(rawRes);
        System.out.println(aggregationRes);

    }
}
