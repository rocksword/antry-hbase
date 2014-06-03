package com.an.antry.hbase.hia;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;

import com.an.antry.hbase.HbaseConnection;

public class HiaMain {
    private static String TABLE_USERS = "users";
    private static String FAMILY_INFO = "info";

    private static String TABLE_TWITS = "twits";
    private static String FAMILY_TWITS = "twits";

    private static String USER_NAME = "TheRealMT";

    private static HConnection connection;

    private static int LONG_LENGTH = Long.SIZE / 8;

    public static void main(String[] args) throws IOException {
        connection = HbaseConnection.getConn();

        boolean init = false;
        if (init) {
            try {
                removeTable(TABLE_TWITS);
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                removeTable(TABLE_USERS);
            } catch (Exception e) {
                e.printStackTrace();
            }
            createTable(TABLE_USERS, FAMILY_INFO);
            createTable(TABLE_TWITS, FAMILY_TWITS);
        }

        // Operate users
        addUsers();
        queryAll(TABLE_USERS);
        readUsers();
        readColumnValue();
        // deleteUsers();
        // deleteColunnValue();
        queryVersionedUserData();

        // Operate twits
        addTwits();
        queryAll(TABLE_TWITS);

        queryTwits(USER_NAME);

        // use the connection for other access to the cluster
        connection.close();
    }

    private static void addTwits() throws IOException {
        System.out.println("Enter addTwits.");

        HTableInterface table = connection.getTable(TABLE_TWITS);

        byte[] userHash = Md5Utils.md5sum(USER_NAME);
        byte[] timestamp = Bytes.toBytes(-1 * 1329088818321L);

        byte[] rowKey = new byte[Md5Utils.MD5_LENGTH + LONG_LENGTH];
        int offset = 0;
        offset = Bytes.putBytes(rowKey, offset, userHash, 0, userHash.length);
        Bytes.putBytes(rowKey, offset, timestamp, 0, timestamp.length);

        Put put = new Put(rowKey);
        put.add(Bytes.toBytes(FAMILY_TWITS), Bytes.toBytes("user"), Bytes.toBytes(USER_NAME));
        put.add(Bytes.toBytes(FAMILY_TWITS), Bytes.toBytes("twit"), Bytes.toBytes("Hello, TwitBase2!"));

        table.put(put);
        table.flushCommits();
        table.close();
    }

    private static void removeTable(String tableName) throws MasterNotRunningException, ZooKeeperConnectionException,
            IOException {
        System.out.println("Enter removeTable: " + tableName);
        Configuration conf = HbaseConnection.getConf();
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        admin.close();
    }

    private static void createTable(String tableName, String familyName) throws MasterNotRunningException,
            ZooKeeperConnectionException, IOException {
        System.out.println("Enter createTable: " + tableName + ", " + familyName);
        Configuration conf = HbaseConnection.getConf();
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor desc = new HTableDescriptor(tableName);
        HColumnDescriptor c = new HColumnDescriptor(familyName);
        c.setMaxVersions(3);
        desc.addFamily(c);
        admin.createTable(desc);
        admin.close();
    }

    // Get cell versioned data
    private static void queryVersionedUserData() throws IOException {
        System.out.println("Enter queryVersionedData");
        HTableInterface table = connection.getTable(TABLE_USERS);
        Get g = new Get(Bytes.toBytes(USER_NAME));
        g.addFamily(Bytes.toBytes(FAMILY_INFO));

        Result r = table.get(g);
        List<Cell> cells = r.getColumnCells(Bytes.toBytes(FAMILY_INFO), Bytes.toBytes("password"));
        System.out.println("Versioned value size: " + cells.size());
        for (Cell cell : cells) {
            System.out.println(cell.getTimestamp() + ", " + Bytes.toString(CellUtil.cloneValue(cell)));
        }
        table.close();
    }

    private static void deleteColunnValue() throws IOException {
        System.out.println("Enter deleteColunnValue");
        HTableInterface table = connection.getTable(TABLE_USERS);
        Delete d = new Delete(Bytes.toBytes(USER_NAME));
        d.deleteColumns(Bytes.toBytes(FAMILY_INFO), Bytes.toBytes("email"));
        table.delete(d);
        table.close();
    }

    // Delete one row data
    private static void deleteUsers() throws IOException {
        System.out.println("Enter deleteUsers");
        Delete d = new Delete(Bytes.toBytes(USER_NAME));
        HTableInterface table = connection.getTable(TABLE_USERS);
        table.delete(d);
        table.close();
    }

    // Save users
    private static void addUsers() throws IOException {
        System.out.println("Enter addUsers");
        // For applications which require high-end multithreaded access
        // Create a connection to the cluster.
        HTableInterface table = connection.getTable(TABLE_USERS);
        Put put = new Put(Bytes.toBytes(USER_NAME));
        put.add(Bytes.toBytes(FAMILY_INFO), Bytes.toBytes("name"), Bytes.toBytes("Mark Twain"));
        put.add(Bytes.toBytes(FAMILY_INFO), Bytes.toBytes("email"), Bytes.toBytes("samuel@clemens.org"));
        put.add(Bytes.toBytes(FAMILY_INFO), Bytes.toBytes("password"), Bytes.toBytes("Langhorne3"));
        table.put(put);
        table.flushCommits();
        table.close();
    }

    // Query all rows
    private static void queryAll(String tableName) throws IOException {
        System.out.println("Enter queryAll: " + tableName);
        HTableInterface table = connection.getTable(tableName);

        ResultScanner rs = table.getScanner(new Scan());

        for (Result r : rs) {
            System.out.println("rowkey: " + new String(r.getRow()));
            for (Cell cell : r.rawCells()) {
                String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println(rowkey + ", " + family + ", " + qualifier + ", " + value);
            }
        }
        table.close();
    }

    private static void queryTwits(String username) throws IOException {
        System.out.println("Enter queryTwits: " + username);
        HTableInterface table = connection.getTable(TABLE_TWITS);

        byte[] userHash = Md5Utils.md5sum(username);
        byte[] startRow = Bytes.padTail(userHash, LONG_LENGTH); // 212d...866f00...
        byte[] stopRow = Bytes.padTail(userHash, LONG_LENGTH);
        stopRow[Md5Utils.MD5_LENGTH - 1]++; // 212d...867000...

        // startRow: [33, 45, -62, 107, 98, 70, -6, -44, -46, -53, 55, 64, -4,
        // 37, -122,
        // 111, 0, 0, 0, 0, 0, 0, 0, 0]
        // stopRow: [33, 45, -62, 107, 98, 70, -6, -44, -46, -53, 55, 64, -4,
        // 37, -122,
        // 112, 0, 0, 0, 0, 0, 0, 0, 0]
        Scan s = new Scan(startRow, stopRow);
        ResultScanner rs = table.getScanner(s);

        for (Result r : rs) {
            // extract the username
            String user = Bytes.toString(r.getValue(Bytes.toBytes(FAMILY_TWITS), Bytes.toBytes("user")));
            // extract the twit
            String message = Bytes.toString(r.getValue(Bytes.toBytes(FAMILY_TWITS), Bytes.toBytes("twit")));
            // extract the timestamp
            byte[] b = Arrays.copyOfRange(r.getRow(), Md5Utils.MD5_LENGTH, Md5Utils.MD5_LENGTH + LONG_LENGTH);
            DateTime dt = new DateTime(-1 * Bytes.toLong(b));
            System.out.println("time: " + dt.toString() + ", user: " + user + ", message: " + message);
        }
        table.close();
    }

    // Read one row data
    private static void readUsers() throws IOException {
        System.out.println("Enter readUsers");
        HTableInterface table = connection.getTable(TABLE_USERS);

        Get g = new Get(Bytes.toBytes(USER_NAME));
        Result r = table.get(g);

        List<Cell> cells = r.listCells();
        for (Cell cell : cells) {
            String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
            String family = Bytes.toString(CellUtil.cloneFamily(cell));
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            System.out.println(rowkey + ", " + family + ", " + qualifier + ", " + value);
        }
        table.close();
    }

    // Read one family data
    private static void readColumnValue() throws IOException {
        System.out.println("Enter readColumnValue");
        HTableInterface table = connection.getTable(TABLE_USERS);

        Get g = new Get(Bytes.toBytes(USER_NAME));
        g.addFamily(Bytes.toBytes(FAMILY_INFO));

        Result r = table.get(g);

        String name = Bytes.toString(r.getValue(Bytes.toBytes(FAMILY_INFO), Bytes.toBytes("name")));
        String email = Bytes.toString(r.getValue(Bytes.toBytes(FAMILY_INFO), Bytes.toBytes("email")));
        String password = Bytes.toString(r.getValue(Bytes.toBytes(FAMILY_INFO), Bytes.toBytes("password")));
        System.out.println("name: " + name + ", email: " + email + ", password: " + password);
        table.close();
    }
}
