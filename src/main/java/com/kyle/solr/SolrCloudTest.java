package com.kyle.solr;


import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SolrCloudTest {

    private static final Logger logger = LoggerFactory.getLogger(SolrCloudTest.class);
    private CloudSolrClient cloudSolrClient;
    private String defaultCollection;
    private String zkHost = "cdh01:2181";
    public SolrCloudTest(String collection){
        this.defaultCollection = collection;
        initClient();
    }

    public void initClient()  {
        final List<String> zkHosts = new ArrayList<>();
        zkHosts.add(zkHost);
        cloudSolrClient = new CloudSolrClient.Builder().withZkHost(zkHosts).build();
        final int zkClientTimeout = 10000;
        final int zkConnectTimeout = 10000;
        cloudSolrClient.setDefaultCollection(defaultCollection);
        cloudSolrClient.setZkClientTimeout(zkClientTimeout);
        cloudSolrClient.setZkConnectTimeout(zkConnectTimeout);
    }

    public void addIndex() throws IOException, SolrServerException {
        ArrayList<SolrInputDocument> docs = new ArrayList<>();
        for (int i = 0; i < 1; i++){
            SolrInputDocument doc = new SolrInputDocument();
            String key = String.valueOf(i);
            doc.addField("rowkey",key);
//            doc.addField("userid",key + "userid");
//            doc.addField("city_id",key + "city_id");
            docs.add(doc);
        }
        logger.info("doc info: " + docs);
        cloudSolrClient.add(docs);
        cloudSolrClient.commit();
    }

    public List<String> serach(String str) throws IOException, SolrServerException {
        ArrayList<String> rowKeys = new ArrayList<>();
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setRows(100);
        solrQuery.setQuery(str);
        logger.info("query str: " + str);
        QueryResponse response = cloudSolrClient.query(solrQuery);
        SolrDocumentList docs = response.getResults();
        System.out.println("文档个数： " + docs.getNumFound());
        for (SolrDocument doc : docs) {
            String rowkey = (String)doc.getFieldValue("rowkey");
            System.out.println("rowkey: " + rowkey);
            rowKeys.add(rowkey);
        }
        return rowKeys;
    }


}































