/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terri.hbase;
import java.io.IOException; 
import java.util.ArrayList; 
import java.util.List; 
 
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.hbase.HBaseConfiguration; 
import org.apache.hadoop.hbase.client.HTable; 
import org.apache.hadoop.hbase.client.Result; 
import org.apache.hadoop.hbase.client.ResultScanner; 
import org.apache.hadoop.hbase.client.Scan; 
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp; 
import org.apache.hadoop.hbase.filter.FilterList.Operator; 
import org.apache.hadoop.hbase.filter.Filter; 
import org.apache.hadoop.hbase.filter.FilterList; 
import org.apache.hadoop.hbase.filter.RegexStringComparator; 
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter; 
import org.apache.hadoop.hbase.util.Bytes; 
 
/**
 * @author XuYi 
 * 
 */ 
public class ScanData { 
 
 public static void main(String[] args) throws IOException { 
  Configuration conf = HBaseConfiguration.create(); 
  conf.set("hbase.zookeeper.quorum", "linux-mint,ubuntu-terri-2-server,ubuntu-terri-1-server"); 
  HTable table = new HTable(conf, "blog"); 
  table.setScannerCaching(5); 
  long begin = System.currentTimeMillis(); 
  Scan scan = new Scan(); 
  scan.setCacheBlocks(true); 
//  scan.addColumn(Bytes.toBytes(""), Bytes.toBytes("")); 
//  scan.setStopRow(Bytes.toBytes("1404696196536_606681636044792")); 
//  Filter filter = new DependentColumnFilter(Bytes.toBytes("f1"), Bytes.toBytes("d1"), true, CompareOp.OP, null); 
  SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("f1"), Bytes.toBytes("d1"), CompareOp.EQUAL, new RegexStringComparator("2013")); 
  filter.setFilterIfMissing(true); 
  List<Filter> list = new ArrayList<Filter>(); 
  list.add(filter); 
  FilterList fl = new FilterList(Operator.MUST_PASS_ALL, list); 
  scan.setFilter(fl); 
  ResultScanner rs = table.getScanner(scan); 
  Result res = null; 
  try { 
   while ((res = rs.next()) != null) { 
//    String rowid = Bytes.toString(res.getRow()); 
//    String value = Bytes.toString(res.getValue(Bytes.toBytes("info"), Bytes.toBytes("message"))); 
    System.out.println(res); 
//    System.out.println(Bytes.toString(res.getValue(Bytes.toBytes("f1"), Bytes.toBytes("d1")))); 
   } 
  } finally { 
   rs.close(); 
  } 
  long end = System.currentTimeMillis(); 
  System.out.println(end - begin); 
 } 
}