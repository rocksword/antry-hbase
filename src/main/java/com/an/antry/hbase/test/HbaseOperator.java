package com.an.antry.hbase.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.an.antry.hbase.HbaseUtil;

public class HbaseOperator {
    private static final Log logger = LogFactory.getLog(HbaseOperator.class);

    public synchronized List<Integer> loadUnfinishedIdRange(String date) {
        try (HConnection conn = HbaseUtil.getConnection();
                HTableInterface table = HbaseUtil.getHTableInterface(conn, HbaseTable.TABLE_URL);) {
            Scan scan = new Scan();
            scan.setCaching(500);
            scan.addColumn(HbaseTable.FAMILY_INFO, HbaseTable.QUALIFIER_ID);
            FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            filters.addFilter(new SingleColumnValueFilter(HbaseTable.FAMILY_INFO, HbaseTable.QUALIFIER_DATE,
                    CompareOp.EQUAL, Bytes.toBytes(date)));
            filters.addFilter(new SingleColumnValueFilter(HbaseTable.FAMILY_INFO, HbaseTable.QUALIFIER_LINK_COUNT,
                    CompareOp.GREATER, Bytes.toBytes(0)));
            filters.addFilter(new SingleColumnValueFilter(HbaseTable.FAMILY_INFO, HbaseTable.QUALIFIER_FINISHED,
                    CompareOp.EQUAL, Bytes.toBytes(false)));
            scan.setFilter(filters);

            ResultScanner rs = table.getScanner(scan);
            Iterator<Result> iterator = rs.iterator();
            long t1 = System.currentTimeMillis();
            Result current = null;
            int minID = -1;
            int maxID = -1;
            while (iterator.hasNext()) {
                current = iterator.next();
                int val = Bytes.toInt(current.getValue(HbaseTable.FAMILY_INFO, HbaseTable.QUALIFIER_ID));
                if (minID == -1 && maxID == -1) {
                    minID = val;
                    maxID = val;
                    logger.info(String.format("minID: %s, maxID: %s", minID, maxID));
                } else {
                    if (val < minID) {
                        minID = val;
                    }
                    if (val > maxID) {
                        maxID = val;
                    }
                }
            }
            long t2 = System.currentTimeMillis();
            logger.info(String.format("time %s", (t2 - t1)));

            if (minID != -1 && maxID != -1) {
                List<Integer> result = new ArrayList<>();
                result.add(minID);
                result.add(maxID);
                logger.info(String.format("Min ID: %s, max ID: %s", minID, maxID));
                return result;
            }
        } catch (Exception e) {
            logger.info("Error: " + e);
        }
        logger.info("Not found url ID range.");
        return null;
    }

    public int loadUnfinishedUrls(String date, boolean finished, boolean log) {
        int count = 0;
        try (HConnection conn = HbaseUtil.getConnection();
                HTableInterface table = HbaseUtil.getHTableInterface(conn, HbaseTable.TABLE_URL);) {

            Scan scan = new Scan();
            scan.setCaching(500);
            FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            filters.addFilter(new SingleColumnValueFilter(HbaseTable.FAMILY_INFO, HbaseTable.QUALIFIER_DATE,
                    CompareOp.EQUAL, Bytes.toBytes(date)));
            filters.addFilter(new SingleColumnValueFilter(HbaseTable.FAMILY_INFO, HbaseTable.QUALIFIER_LINK_COUNT,
                    CompareOp.GREATER, Bytes.toBytes(0)));
            filters.addFilter(new SingleColumnValueFilter(HbaseTable.FAMILY_INFO, HbaseTable.QUALIFIER_FINISHED,
                    CompareOp.EQUAL, Bytes.toBytes(finished)));
            scan.setFilter(filters);

            ResultScanner rs = table.getScanner(scan);
            Iterator<Result> iterator = rs.iterator();

            long t1 = System.currentTimeMillis();
            List<Integer> ids = new ArrayList<>();
            while (iterator.hasNext()) {
                Result last = iterator.next();
                if (log) {
                    int id = Bytes.toInt(last.getValue(HbaseTable.FAMILY_INFO, HbaseTable.QUALIFIER_ID));
                    ids.add(id);
                }
                count++;
            }
            Collections.sort(ids);
            if (log) {
                StringBuilder builder = new StringBuilder();
                for (Integer id : ids) {
                    builder.append(id).append(",");
                }
                logger.info(builder.toString());
            }
            long t2 = System.currentTimeMillis();
            logger.info(String.format("time %s", (t2 - t1)));
        } catch (Exception e) {
            logger.info("Error: " + e);
        }
        logger.info("Not finished urls count: " + count);
        return count;
    }

    public int loadMaxId(String date) {
        int maxId = -1;
        try (HConnection conn = HbaseUtil.getConnection();
                HTableInterface table = HbaseUtil.getHTableInterface(conn, HbaseTable.TABLE_URL);) {

            Scan scan = new Scan();
            scan.setCaching(500);
            scan.addColumn(HbaseTable.FAMILY_INFO, HbaseTable.QUALIFIER_ID);
            FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            filters.addFilter(new SingleColumnValueFilter(HbaseTable.FAMILY_INFO, HbaseTable.QUALIFIER_DATE,
                    CompareOp.EQUAL, Bytes.toBytes(date)));
            filters.addFilter(new SingleColumnValueFilter(HbaseTable.FAMILY_INFO, HbaseTable.QUALIFIER_LINK_COUNT,
                    CompareOp.GREATER, Bytes.toBytes(0)));
            filters.addFilter(new SingleColumnValueFilter(HbaseTable.FAMILY_INFO, HbaseTable.QUALIFIER_FINISHED,
                    CompareOp.EQUAL, Bytes.toBytes(false)));
            scan.setFilter(filters);

            ResultScanner rs = table.getScanner(scan);
            Iterator<Result> iterator = rs.iterator();

            long t1 = System.currentTimeMillis();
            Result last = null;
            while (iterator.hasNext()) {
                last = iterator.next();
            }
            if (last != null) {
                long t2 = System.currentTimeMillis();
                maxId = Bytes.toInt(last.getValue(HbaseTable.FAMILY_INFO, HbaseTable.QUALIFIER_ID));
                rs.close();
                long t3 = System.currentTimeMillis();
                logger.info(String.format("time 1 %s, time 2 %s", (t2 - t1), (t3 - t2)));
            } else {
                logger.info("Not found links that need to be checked.");
            }
        } catch (Exception e) {
            logger.info("Error: " + e);
        }
        return maxId;
    }
}
