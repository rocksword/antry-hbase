package com.an.antry.hbase;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class HbaseConnectionTest {
    @Test
    public void test() {
        HbaseConnection conn = new HbaseConnection();
        Configuration conf = conn.getConf();
        System.out.println(conf);
    }
}
