/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terri.hbase;


import static com.terri.hbase.TestPv.connection;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
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

/**
 *
 * @author terri
 */
public class HbaseUtil {

    static final Configuration cfg = HBaseConfiguration.create();
    static Connection connection;

    static {
        cfg.set("hbase.zookeeper.property.clientPort", "2181");
        cfg.set("hbase.zookeeper.quorum", "172.16.2.41,172.16.2.42,172.16.2.43");
       // cfg.set("hbase.master", "airmediahbasev3.azurehdinsight.cn:60000");
        try {
            connection = ConnectionFactory.createConnection(cfg);
        } catch (IOException ex) {
            Logger.getLogger(HbaseUtil.class.getName()).log(Level.SEVERE, "初始化configuration异常", ex);
        }
    }

    public static void put(String tablename, List<Put> list) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tablename));
        table.put(list);
        System.out.println("put "+list.size());
    }
    public static Result get(String tablename, String row) throws Exception {
        Table table=connection.getTable(TableName.valueOf(tablename));
        Get get = new Get(Bytes.toBytes(row));
        Result result = table.get(get);
        return result;
        // System.out.println("Get: " + result);
    }

    public static void scan(String tableName) throws Exception {
        Table table=connection.getTable(TableName.valueOf(tableName));
        Scan s = new Scan();
        ResultScanner rs = table.getScanner(s);

        for (Result r : rs) {
            System.out.println("Scan: " + r);

        }
    }

     public static void splitTable(String tableName, String... splits) throws InterruptedException,
            IOException {
        // split table
        if (null != splits && splits.length > 0) {
            // wait for the table settle down
            Thread.sleep(6000);
            Admin admin = connection.getAdmin();
            TableName table = TableName.valueOf(tableName);
            List<HRegionInfo> regions = null;
            for (int a = 0; a < splits.length; a++) {
                admin.split(table, Bytes.toBytes(splits[a]));
                Thread.sleep(6000);

                regions = admin.getTableRegions(table);
                for (HRegionInfo region : regions) {
                    while (region.isOffline()) {
                        // wait region online
                    }
                    System.out.println("region:" + region);
                    System.out.println("startKey:" + Bytes.toString(region.getStartKey()));
                    System.out.println("endKey:" + Bytes.toString(region.getEndKey()));
                }
            }
            admin.balancer();
        }
    }
}
