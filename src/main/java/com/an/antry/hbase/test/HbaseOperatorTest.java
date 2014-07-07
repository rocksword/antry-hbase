package com.an.antry.hbase.test;

import java.util.List;

import org.junit.Test;

public class HbaseOperatorTest {
    public void testLoadMaxId() {
        HbaseOperator oper = new HbaseOperator();
        int madId = oper.loadMaxId("2014-06-02");
        System.out.println(madId);
    }

    @Test
    public void testLoadUnfinishedUrls() {
        HbaseOperator oper = new HbaseOperator();
        int count = oper.loadUnfinishedUrls("2014-06-02", true, true);
        System.out.println(count);
    }

    public void testLoadUnfinishedIdRange() {
        HbaseOperator oper = new HbaseOperator();
        List<Integer> range = oper.loadUnfinishedIdRange("2014-06-02");
    }
}
