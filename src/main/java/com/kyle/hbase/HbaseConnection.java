package com.kyle.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HbaseConnection {

    private Connection connection;
    private Configuration conf;


    public HbaseConnection(){
        getConnection();
    }


    private Configuration getConf(){
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","cdh01:2181");
        return conf;
    }

    public Connection getConnection(){
        if (connection == null){
            reconnect();
        }
        return connection;
    }

    private void reconnect(){
        try{
            if (null != connection){
                connection.close();
            }
            if (null == conf){
                getConf();
            }
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



}
