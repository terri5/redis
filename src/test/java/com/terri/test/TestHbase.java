/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terri.test;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.terri.hbase.HbaseUtil;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import com.sun.rowset.CachedRowSetImpl;
import org.apache.hadoop.hbase.util.Bytes;
import  com.microsoft.sqlserver.jdbc.SQLServerDriver;

/**
 *
 * @author GZETL
 */
public class TestHbase {

    static final Configuration cfg = HBaseConfiguration.create();
    static Connection connection;

    public TestHbase() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
        cfg.set("hbase.zookeeper.property.clientPort", "2181");
        cfg.set("hbase.zookeeper.quorum", "172.16.2.41,172.16.2.42,172.16.2.43");
        // cfg.set("hbase.master", "airmediahbasev3.azurehdinsight.cn:60000");
        try {
            connection = ConnectionFactory.createConnection(cfg);
        } catch (IOException ex) {
            Logger.getLogger(HbaseUtil.class.getName()).log(Level.SEVERE, "初始化configuration异常", ex);
        }
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testRead() {
        try {
            Result rst = HbaseUtil.get("DEVICE_LOG_APP", "20161109");
            System.out.println("rst: " + rst.size());
        } catch (Exception ex) {
            Logger.getLogger(TestHbase.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Test
    public void testScan() {
        try {

            Table table = connection.getTable(TableName.valueOf("DEVICE_LOG_APP"));

            Filter filter1 = new RowFilter(CompareFilter.CompareOp.EQUAL,
                    new BinaryPrefixComparator(Bytes.toBytes("20161109")));
            // EQUAL 

            Scan s = new Scan();
            s.setFilter(filter1);
            ResultScanner rs = table.getScanner(s);

            for (Result r : rs) {
                System.out.println("rowkey:" + new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("列族:" + new String(keyValue.getFamily())
                            + " 列:" + new String(keyValue.getQualifier()) + ":"
                            + new String(keyValue.getValue()));
                }
            }

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();

        }
    }
    @Test
    public void testBatchInsert() {
        SQLServerBulkCopy bulk = null;
        String driverName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";//加载驱动程序 
        String url = "jdbc:sqlserver://amsqldwserver2016.database.chinacloudapi.cn:1433;databaseName=amwifiboxsqldw;sendStringParametersAsUnicode=false;";
        String user = "amsqldwsa";
        String password = "Password!23";
        ResultSet rs = null;

        try {
            Class.forName(driverName);  
            java.sql.Connection con = DriverManager.getConnection(user, user, password);

            java.sql.Statement stmt = con.createStatement();

            rs = stmt.executeQuery("select top 0 * from report.bi_coolest_v2_url_test");

            bulk = new SQLServerBulkCopy(url + "user=" + user + ";password=" + password);

            bulk.setDestinationTableName("report.bi_coolest_v2_url_test");

            for (int k = 0; k < 10; k++) {

                try (CachedRowSetImpl x = new CachedRowSetImpl()) {
                    for (int i = 0; i < 1000; i++) {
                        
                        x.populate(rs);
                        
                        x.moveToInsertRow();
                        
                        x.updateInt(1, 20161130);
                        
                        x.updateString(2, "1");
                        
                        x.updateInt(3, -1);
                        
                        x.updateInt(4, -1);
                        
                        x.updateInt(5, -1);
                        
                        x.updateInt(6, 8);
                        
                        x.updateString(7, "abc");
                        
                        x.updateInt(8, 0);
                        
                        x.updateInt(9, 8);
                        
                        /*
                        
                        * x.updateString(1, "fff"); x.updateInt(2, 999);
                        
                        * x.updateString(3, "def");
                        
                        */
                        x.insertRow();
                        
                        x.moveToCurrentRow();
                        
                    }
                    
                    // bulk.setDestinationTableName("report.test_bulkcopy");
                    System.out.println(Instant.now());
                    
                    bulk.writeToServer(x);
                    
                    System.out.println(Instant.now());
                    
                    System.out.println();
                }

            }

        } catch (Exception e) {
            System.err.println("msg:"+e.getMessage());
            e.printStackTrace();
        } finally {
           if(bulk!=null)
            bulk.close();

            try {
                if(rs!=null)
                rs.close();
            } catch (SQLException ex) {
                Logger.getLogger(TestHbase.class.getName()).log(Level.SEVERE, null, ex);
            }

        }

    }
    // TODO add test methods here.
    // The methods must be annotated with annotation @Test. For example:
    //
    // @Test
    // public void hello() {}
}
