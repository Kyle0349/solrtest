package com.kyle.hbase;

import com.alibaba.fastjson.JSON;
import com.kyle.solr.SolrCloudTest;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrServerException;

import java.io.IOException;
import java.util.*;

public class HbaseQuery {
    //加载配置文件属性
    static Config config = ConfigFactory.load("userdev_pi_solr.properties");
    private HbaseConnection connection = new HbaseConnection(config.getString("hbase_zookeeper_quorum"));

    public String getDataFromHbase(String tableName, String solr_collection, String searchStr) throws IOException, SolrServerException {
        SolrCloudTest solrCloudTest = new SolrCloudTest(solr_collection);
        List<String> rowKeys = solrCloudTest.serach(searchStr);
        Connection conn = connection.getConnection();
        Table table = conn.getTable(TableName.valueOf(tableName));
        Result[] results = table.get(listGet(rowKeys));
        List<Map<String, Map<String, String>>> maps = resultHandler(Arrays.asList(results));
        return JSON.toJSONString(maps);
    }


    private List<Get> listGet(List<String> rowKeys){
        ArrayList<Get> gets = new ArrayList<>();
        for (String rowKey : rowKeys) {
            gets.add(new Get(Bytes.toBytes(rowKey)));
        }
        return gets;
    }


    private List<Map<String,Map<String,String>>> resultHandler(List<Result> results){
        ArrayList<Map<String, Map<String, String>>> rsMaps = new ArrayList<>();
        for (Result result : results) {
            if (!result.isEmpty()){
                Map<String, Map<String, String>> row = new HashMap<>();
                HashMap<String, String> kvMap = new HashMap<>();
                row.put(new String(result.getRow()),kvMap);
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    kvMap.put(new String(CellUtil.cloneQualifier(cell)),new String(CellUtil.cloneValue(cell)));
                }
                rsMaps.add(row);
            }
        }
        return rsMaps;
    }


}
