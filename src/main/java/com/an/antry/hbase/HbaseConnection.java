package com.an.antry.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;

public class HbaseConnection {
    private static String slaves_1 = "ss1,ss2,ss3";
    public static String slaves_2 = "bigdata-master-2131,bigdata-master-2132,bigdata-master-2133,bigdata-master-2170,bigdata-master-2171";

    public synchronized static Configuration getConf() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", slaves_1);
        return conf;
    }

    public synchronized static HConnection getConn() throws IOException {
        HConnection conn = HConnectionManager.createConnection(getConf());
        return conn;
    }

    private static HConnection hbaseConn = null;

    public static HTableInterface getHTable(String table) throws IOException {
        if (hbaseConn == null) {
            synchronized (HbaseConnection.class) {
                if (hbaseConn == null) {
                    Configuration conf = HBaseConfiguration.create();
                    conf.set("hbase.zookeeper.quorum", slaves_2);
                    hbaseConn = HConnectionManager.createConnection(conf);
                }
            }
        }
        return hbaseConn.getTable(table);
    }
}
