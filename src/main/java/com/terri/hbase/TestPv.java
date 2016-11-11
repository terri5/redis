/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terri.hbase;

/**
 *
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import sun.misc.BASE64Encoder;


public class TestPv {
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            // TODO Auto-generated method stub
            /**
             * 注意，需要启动所有的zookeeper.
             */
            String tableName = "DEVICE_LOG_PV";
            String columnFamily = "PV";
          
            if (true == TestPv.delete(tableName)) {
                System.out.println("Delete Table " + tableName + " success!");
                TestPv.create(tableName, columnFamily);
                System.out.println("balance over");
                Thread.sleep(1000*2);
              //  System.exit(0);
                long start = System.currentTimeMillis();
                String fileName = "E:\\BaiduYunDownload\\pv.txt";
                File file = new File(fileName);

                System.out.println("以行为单位读取文件内容，一次读一整行：");
                try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    String tmpStr = null;
                    int line = 0;
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    List<Put> list = new ArrayList<>();

                    // 一次读入一行，直到读入null为文件结束
                    while ((tmpStr = reader.readLine()) != null) {
                        // 显示行号
                        String[] arr = tmpStr.split("\t");
                        if(arr.length<13) continue;
                      
                           // String row = getHbaseRowKeyUnique(sdf.parse(arr[3]), arr[0]);
                            String row = getRowKey(arr);
                            Put put = new Put(Bytes.toBytes(row));
                            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("DMAC"), Bytes.toBytes(arr[0]));
                            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("MAC"), Bytes.toBytes(arr[1])); 
                            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("IP"), Bytes.toBytes(arr[2]));
                            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("DATETIME_POINT"), Bytes.toBytes(arr[3]));
                            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("CLIENT_AGENT"), Bytes.toBytes(arr[4]));
                            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("HTTP_METHOD"), Bytes.toBytes(arr[5]));
                            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("HTTP_URI"), Bytes.toBytes(arr[6]));
                            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("HTTP_VERSION"), Bytes.toBytes(arr[7]));
                            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("REFERER"), Bytes.toBytes(arr[8]));
                            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("CLIENT_OS"), Bytes.toBytes(arr[9]));
                            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("MOBILE_BRAND"), Bytes.toBytes(arr[10]));
                            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("CLIENT_BROWSER"), Bytes.toBytes(arr[11]));
                            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("DAY_ID"), Bytes.toBytes(arr[12]));
                            list.add(put);
                            line++;
                            if (line % 100000 == 0) {
                                 long start1 = System.currentTimeMillis();
                                puts(tableName, list);
                                list = new ArrayList();
                                long end1 = System.currentTimeMillis();
                                System.out.println("总耗时" + (end1- start1) / 1000 + "秒,写入" + line + "行");
                            }
                        if(line>3000000) break;
                    }
                    long end = System.currentTimeMillis();
                    System.out.println("总耗时" + (end - start) / 1000 + "秒,写入" + line + "行");
                }

                create(tableName, columnFamily);
            }

            //       TestPv.get(tableName, "row1");
            //        TestPv.scan(tableName);
        } catch (IOException ex) {
            Logger.getLogger(TestPv.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(TestPv.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
    

    static final Configuration cfg = HBaseConfiguration.create();
    static  Connection connection ;

    static {
        cfg.set("hbase.zookeeper.property.clientPort", "2181");
        cfg.set("hbase.zookeeper.quorum", "linux-mint,ubuntu16-1,ubuntu16-2");
        cfg.set("hbase.master", "linux-mint:60000");
        try {
            connection = ConnectionFactory.createConnection(cfg);
        } catch (IOException ex) {
            Logger.getLogger(TestPv.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static String getHbaseRowKeyUnique(Date businessTime, String dmac) {
        UUID uuid = UUID.randomUUID();
        int rand = (int) (Math.random() * 10);
        Date now = Calendar.getInstance().getTime();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat sdfhm = new SimpleDateFormat("HHmm");
        SimpleDateFormat sdfb = new SimpleDateFormat("yyyyMMddHHmm");

        return sdf.format(now) + rand + sdfhm.format(now) + dmac + sdfb.format(businessTime) + uuid.toString().substring(0, 6).toUpperCase();
    }
    public static String getRowKey(String[] arr)
    {
        String time=arr[3].replace("-","").replace(" ","").replace(":","");
        return time.substring(0,8)+time.substring(12,14)+time.substring(8,12)  +arr[1].replace(":", "") + arr[0] + EncoderByMd5(arr[6]+arr[7]).substring(0,8).replace("/", "").replace("+","");
    }
    public static String EncoderByMd5(String str){
        try {
            //确定计算方法
            MessageDigest md5=MessageDigest.getInstance("MD5");
            BASE64Encoder base64en = new BASE64Encoder();
            //加密后的字符串
            String newstr=base64en.encode(md5.digest(str.getBytes("utf-8")));
            return newstr;
        } catch (NoSuchAlgorithmException ex) {
            Logger.getLogger(TestPv.class.getName()).log(Level.SEVERE, null, ex);
        } catch (UnsupportedEncodingException ex) {
            Logger.getLogger(TestPv.class.getName()).log(Level.SEVERE, null, ex);
        }
        return "";
    }
    public static void create(String tableName, String columnFamily)
            throws Exception {
    
         Admin admin = connection.getAdmin();
        TableName Tname=TableName.valueOf(tableName);
        if (admin.tableExists(Tname)) {
            System.out.println(tableName + " exists!");
        } else {

            HTableDescriptor tableDesc;
            tableDesc = new HTableDescriptor(Tname);
            tableDesc.addFamily(new HColumnDescriptor(columnFamily));
          // admin.createTable(tableDesc,Bytes.toBytes("2016081000"),Bytes.toBytes("2016081059"),60);
            admin.createTable(tableDesc,getSplits(60));
           //admin.createTable(tableDesc);
           //admin.split(tableDesc.getTableName(),Bytes.toBytes("2016081030")); 
           admin.balancer();
            System.out.println(tableName + " create successfully!");
        }
    }
    
     public static byte[][] getSplits(int numRegions) {
       byte[][] splits = new byte[numRegions-1][];
       for(int i=1;i<numRegions;i++){
          byte[] b;
           b = Bytes.toBytes("20160810"+(i<10 ?"0"+i:""+i));
          splits[i-1] = b;
       }
       return splits; 
     }
     
     public static byte[][] getSplits() {
       byte[][] splits = new byte[19][];
       for(int i=0;i<60;i++){
          byte[] b;
          if(i%3==0){
             b = Bytes.toBytes("20160810"+(i<10 ?"0"+i:""+i));
             splits[i/3] = b;
          }
        
       }
       return splits; 
     }
         
    public static void puts(String tablename, List<Put> list) throws Exception {
        //HTable table = new HTable(cfg, tablename);
        Table table=connection.getTable(TableName.valueOf(tablename));
        table.put(list);
        System.out.println("put " + list.size());

    }
    

    public static void put(String tablename, String row, String columnFamily,
            String column, String data) throws Exception {

        Table table=connection.getTable(TableName.valueOf(tablename));
        Put put = new Put(Bytes.toBytes(row));

        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
                Bytes.toBytes(data));

        table.put(put);

        System.out.println("put '" + row + "', '" + columnFamily + ":" + column
                + "', '" + data + "'");

    }

    public static void get(String tablename, String row) throws Exception {
        Table table=connection.getTable(TableName.valueOf(tablename));
        Get get = new Get(Bytes.toBytes(row));
        Result result = table.get(get);
        System.out.println("Get: " + result);
    }

    public static void scan(String tableName) throws Exception {
        Table table=connection.getTable(TableName.valueOf(tableName));
        Scan s = new Scan();
        ResultScanner rs = table.getScanner(s);

        for (Result r : rs) {
            System.out.println("Scan: " + r);

        }
    }

    public static boolean delete(String tableName) {

        try {
            Admin admin = connection.getAdmin();
            if (admin.tableExists(TableName.valueOf(tableName))) {
                admin.disableTable(TableName.valueOf(tableName));
                admin.deleteTable(TableName.valueOf(tableName));
            }
            return true;
        } catch (IOException ex) {
            Logger.getLogger(TestPv.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }
    
    public static byte[][] getHexSplits(String startKey, String endKey, int numRegions) {
        byte[][] splits = new byte[numRegions-1][];
        BigInteger lowestKey = new BigInteger(startKey, 16);
        BigInteger highestKey = new BigInteger(endKey, 16);
        BigInteger range = highestKey.subtract(lowestKey);
        BigInteger regionIncrement = range.divide(BigInteger.valueOf(numRegions));
        lowestKey = lowestKey.add(regionIncrement);
        for(int i=0; i < numRegions-1;i++) {
          BigInteger key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)));
          byte[] b = String.format("%016x", key).getBytes();
          splits[i] = b;
        }
  return splits;
}
}
