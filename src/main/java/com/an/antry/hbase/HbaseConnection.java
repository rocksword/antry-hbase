package com.an.antry.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

public class HbaseConnection {
    private static String slaves_1 = "ss1,ss2,ss3";
    private static String slaves_2 = "s1,s2,s3";

    public synchronized static Configuration getConf() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", slaves_1);
        return conf;
    }

    public synchronized static HConnection getConn() throws IOException {
        HConnection conn = HConnectionManager.createConnection(getConf());
        return conn;
    }
}
