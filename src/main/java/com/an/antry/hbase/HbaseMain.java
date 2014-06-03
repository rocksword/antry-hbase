package com.an.antry.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseMain {

    public static void main(String[] args) {
        HbaseMain main = new HbaseMain();
        main.run();
    }

    private void run() {
        Configuration conf = HbaseConnection.getConf();

        HTableInterface table = null;
        try {
            table = new HTable(conf, "mytable");
        } catch (IOException e) {
            e.printStackTrace();
        }

        Get get = new Get(Bytes.toBytes("first"));
        Result r = null;
        try {
            r = table.get(get);
        } catch (IOException e) {
            e.printStackTrace();
        }

        List<Cell> cells = r.getColumnCells(Bytes.toBytes("cf"), Bytes.toBytes("data"));
        for (Cell c : cells) {
            System.out.println(c.getTimestamp() + ", " + Bytes.toString(CellUtil.cloneValue(c)));
        }

        String val = Bytes.toString(r.getValue(Bytes.toBytes("cf"), Bytes.toBytes("data")));
        System.out.println(val);

        try {
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
