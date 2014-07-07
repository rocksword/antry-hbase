package com.an.antry.hbase.test;

import org.apache.hadoop.hbase.util.Bytes;

public class HbaseTable {
    public static byte[] FAMILY_INFO = Bytes.toBytes("info");

    public static byte[] TABLE_URL = Bytes.toBytes("url");
    public static byte[] QUALIFIER_ID = Bytes.toBytes("id");
    public static byte[] QUALIFIER_DATE = Bytes.toBytes("date");
    public static byte[] QUALIFIER_NAME = Bytes.toBytes("name");
    public static byte[] QUALIFIER_DEPTH = Bytes.toBytes("depth");
    public static byte[] QUALIFIER_LINK_COUNT = Bytes.toBytes("linkCount");
    public static byte[] QUALIFIER_RAW_LINKS = Bytes.toBytes("rawlinks");
    public static byte[] QUALIFIER_FINISHED = Bytes.toBytes("finished");

    public static byte[] TABLE_TARGET = Bytes.toBytes("target");
    public static byte[] QUALIFIER_SOURCE_URL = Bytes.toBytes("sourceurl");
}
