package iotdb.test;

import org.apache.iotdb.tsfile.read.filter.operator.In;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SameTest {
    public static void main(String[] args) {
        HashMap<String, Integer> map = new HashMap<>();

        map.put("AAA",1);
        map.put("Ajt",2);
        map.put("fcy",3);
        map.put("jAA",4);
        map.put("ydA",5);
        String[] s = {"AAA","Ajt","fcy","jAA","ydA"};
        Arrays.sort(s);
        for(String i:s){
            System.out.println(i);
        }
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
        }
//        System.out.println(s.toString());
    }
}
