package com.kyle.hbase;

import com.kyle.solr.SolrIndexTools;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import com.typesafe.config.Config;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.List;

public class UserDevPiSolrObserver extends BaseRegionObserver {

    //加载配置文件属性
    static Config config = ConfigFactory.load("userdev_pi_solr.properties");
    //log记录

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e,
                        Put put, WALEdit edit, Durability durability) throws IOException {
        //获取行健
        String rowkey = Bytes.toString(put.getRow());
        //实例化solrDoc
        SolrInputDocument doc = new SolrInputDocument();
        //添加solr uniquekey值
        doc.addField("rowkey",rowkey);
        //获取需要索引的列
        String[] hbase_columns = config.getString("hbase_column").split(",");
        String hbase_column_family = config.getString("hbase_column_family");
        //获取需要索引的列的值并将其添加到SolrDoc
        for (String colName : hbase_columns) {
            String colValue = "";
            //获取指定列
            List<Cell> cells = put.get(hbase_column_family.getBytes(), colName.getBytes());
            if (null != cells){
                try{
                    colValue = Bytes.toString(CellUtil.cloneValue(cells.get(0)));
                }catch (Exception e1){
                    e1.printStackTrace();
                }
            }
            doc.addField(colName,colValue);
        }
        SolrIndexTools.addDoc(doc);
    }

    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e,
                           Delete delete, WALEdit edit, Durability durability) throws IOException {
        //得到rowkey
        String rowkey = Bytes.toString(delete.getRow());
        //发送数据到本地缓存
        String solr_collection = config.getString("solr_collection");
        SolrIndexTools.delDoc(rowkey);
    }
}
