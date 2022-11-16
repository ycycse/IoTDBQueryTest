package iotdb.test;

import org.apache.iotdb.jdbc.IoTDBStatement;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.OldSessionDataSet;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.session.util.Version;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class PerformanceTest2 {

    static double JDBCTest1(String sql) throws ClassNotFoundException, SQLException {
        try(Connection iotDBConnection = DriverManager.getConnection("jdbc:iotdb://"+Config.IP+":"+Config.PORT+"/",Config.USERNAME,Config.PASSWORD);
            IoTDBStatement statement = (IoTDBStatement) iotDBConnection.createStatement()){
            statement.setFetchSize(5000);
            long Start = System.nanoTime();
            ResultSet resultSet = statement.executeQuery(sql);
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int colCount = resultSetMetaData.getColumnCount();
            while(resultSet.next()){
                for(int i=1;i<=colCount;i++){
                    resultSet.getString(i);
                }
            }
            long End = System.nanoTime();
            return (double) (End - Start) / 1e6;
        }
    }
    static double JDBCTest2(String sql) throws ClassNotFoundException, SQLException {
        try (Connection iotDBConnection = DriverManager.getConnection("jdbc:iotdb://" + Config.IP + ":" + Config.PORT + "/", Config.USERNAME, Config.PASSWORD);
             IoTDBStatement statement = (IoTDBStatement) iotDBConnection.createStatement()) {
            long Start = System.nanoTime();
            ResultSet resultSet = statement.OldexecuteQuery(sql);
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int colCount = resultSetMetaData.getColumnCount();
            while (resultSet.next()) {
                for (int i = 1; i <= colCount; i++) {
                    resultSet.getString(i);
                }
            }
            long End = System.nanoTime();
            return (double) (End - Start) / 1e6;
        }
    }
    static double SessionTest1(String sql){
        try(Session session = new Session(Config.SESSION_IP, Config.PORT, Config.USERNAME, Config.PASSWORD)) {
            session.open(false);

            long newStart = System.nanoTime();
            SessionDataSet sessionDataSet =
                    session.executeQueryStatement(sql,10000);
//            while (sessionDataSet.hasNext())sessionDataSet.next();
            SessionDataSet.DataIterator dataIterator = sessionDataSet.iterator();
            int columnCount = sessionDataSet.getColumnNames().size();
            while(dataIterator.next()){
                for(int i=1;i<=columnCount;i++){
                    dataIterator.getString(i);
                }
            }
            long newEnd = System.nanoTime();
            return (double) (newEnd - newStart) / 1e6;
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            e.printStackTrace();
        }
        return -1;
    }
    static double SessionTest2(String sql){
        try(Session session = new Session(Config.SESSION_IP, Config.PORT, Config.USERNAME, Config.PASSWORD)) {
            session.open(false);
            long newStart = System.nanoTime();
            OldSessionDataSet oldsessionDataSet =
                    session.OldexecuteQueryStatement(sql, 10000);
//            while(oldsessionDataSet.hasNext())oldsessionDataSet.next();
            OldSessionDataSet.DataIterator dataIterator = oldsessionDataSet.iterator();
            int columnCount = oldsessionDataSet.getColumnNames().size();
            while(dataIterator.next()){
                for(int i=1;i<=columnCount;i++){
                    dataIterator.getString(i);
                }
            }
            long newEnd = System.nanoTime();
            return (double) (newEnd - newStart) / 1e6;
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            e.printStackTrace();
        }
        return -1;
    }
}
