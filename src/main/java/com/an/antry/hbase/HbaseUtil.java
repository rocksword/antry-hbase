package com.an.antry.hbase;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.mapreduce.Job;

public class HbaseUtil {
    private static final Log logger = LogFactory.getLog(HbaseUtil.class);
    private static String hbaseConnConf = "hbase-conn.xml";

    private static Configuration conf = new Configuration();
    private static volatile boolean finishedAddResources = false;

    /**
     * @return
     */
    public synchronized static Configuration getConf() {
        if (!finishedAddResources) {
            logger.info("conf: " + conf);
            try {
                String confFilePath = "D:\\github\\antry-hbase\\conf\\";
                conf.addResource(new FileInputStream(new File(confFilePath + hbaseConnConf)));
            } catch (FileNotFoundException e) {
                logger.info("Error: " + e);
            }

            logger.info("After add resources: " + conf);
            finishedAddResources = true;
        }
        return conf;
    }

    /**
     * @param conn
     * @param tableName
     * @return
     * @throws IOException
     */
    public synchronized static HTableInterface getHTableInterface(HConnection conn, byte[] tableName)
            throws IOException {
        return conn.getTable(tableName);
    }

    /**
     * @return
     * @throws MasterNotRunningException
     * @throws ZooKeeperConnectionException
     * @throws IOException
     */
    public synchronized static HBaseAdmin getHBaseAdmin() throws MasterNotRunningException,
            ZooKeeperConnectionException, IOException {
        return new HBaseAdmin(getConf());
    }

    /**
     * @param jobName
     * @return
     * @throws IOException
     */
    public synchronized static Job getJob(String jobName) throws IOException {
        return Job.getInstance(getConf(), jobName);
    }

    /**
     * @return
     * @throws IOException
     */
    public synchronized static HConnection getConnection() throws IOException {
        HConnection conn = HConnectionManager.createConnection(getConf());
        return conn;
    }

    /**
     * If table exists, it will disable and drop the table before recreating it
     * with column family names
     * 
     * @param tableName
     * @param columnFamilyNames
     */
    public synchronized static void checkTableAndOverrideIfExists(String tableName, byte[]... columnFamilyNames) {
        try {
            HBaseAdmin admin = HbaseUtil.getHBaseAdmin();

            try {
                if (admin.tableExists(tableName)) {
                    admin.disableTable(tableName);
                    admin.deleteTable(tableName);
                    logger.info("Deleting table: " + tableName);
                }

                HTableDescriptor thisTable = new HTableDescriptor(TableName.valueOf(tableName));
                for (byte[] fimilyName : columnFamilyNames) {
                    HColumnDescriptor family = new HColumnDescriptor(fimilyName);
                    thisTable.addFamily(family);
                }

                admin.createTable(thisTable);
                logger.info("Creating table: " + tableName);

            } finally {
                admin.close();
            }

        } catch (Exception e) {
            logger.error("Error while checking table " + tableName + ", " + e);
        }
    }
}
