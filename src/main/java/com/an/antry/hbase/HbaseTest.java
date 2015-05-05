package com.an.antry.hbase;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseTest {
    public static void main(String[] args) throws IOException {
        // keyScan();
        columnFilter();
    }

    private static void rowFilter() throws IOException {
        String fromKey = "0000034EAE0C";
        HConnection conn = null;
        try {
            conn = getConn();
            HTableInterface table = conn.getTable("snhit_summary");
            Scan scan = new Scan(Bytes.toBytes(fromKey), Bytes.toBytes(fromKey));
            try (ResultScanner rs = table.getScanner(scan);) {
                for (Result r = rs.next(); r != null; r = rs.next()) {
                    for (Entry<byte[], byte[]> entry : r.getFamilyMap(Bytes.toBytes("cf")).entrySet()) {
                        System.out.println(new String(entry.getKey()) + ", " + new String(entry.getValue()));
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            conn.close();
        }
    }

    private static void keyScan() throws IOException {
        String fromKey = "0000034EAE0C";
        HConnection conn = null;
        try {
            conn = getConn();
            HTableInterface table = conn.getTable("snhit_summary");
            Scan scan = new Scan(Bytes.toBytes(fromKey), Bytes.toBytes(fromKey));
            try (ResultScanner rs = table.getScanner(scan);) {
                for (Result r = rs.next(); r != null; r = rs.next()) {
                    for (Entry<byte[], byte[]> entry : r.getFamilyMap(Bytes.toBytes("cf")).entrySet()) {
                        System.out.println(new String(entry.getKey()) + ", " + new String(entry.getValue()));
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            conn.close();
        }
    }

    private static void columnFilter() throws IOException {
        String startDate = "2014-04-19";
        String endDate = "2014-04-19";
        HConnection hbaseConn = null;
        try {
            hbaseConn = getConn();
            HTableInterface table = hbaseConn.getTable("snhit_summary");
            Scan scan = new Scan();
            Filter filter = new ColumnRangeFilter(Bytes.toBytes(startDate), true, Bytes.toBytes(endDate), true);
            scan.setFilter(filter);
            try (ResultScanner rs = table.getScanner(scan);) {
                int cnt = 0;
                Set<String> snSet = new HashSet<>();
                for (Result r = rs.next(); r != null; r = rs.next()) {
                    for (Entry<byte[], byte[]> entry : r.getFamilyMap(Bytes.toBytes("cf")).entrySet()) {
                        String sn = new String(r.getRow());
                        System.out
                                .println(sn + ", " + new String(entry.getKey()) + ", " + new String(entry.getValue()));
                        snSet.add(sn);
                        cnt++;
                    }
                }
                System.out.println("Total row count: " + cnt);
                System.out.println("Total sn count: " + snSet.size());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            hbaseConn.close();
        }
    }

    private static HConnection getConn() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", HbaseConnection.slaves_2);
        HConnection hbaseConn = HConnectionManager.createConnection(conf);
        return hbaseConn;
    }
}
