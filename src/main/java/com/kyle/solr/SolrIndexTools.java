package com.kyle.solr;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Semaphore;

public class SolrIndexTools {

    //加载配置文件属性
    static Config config = ConfigFactory.load("userdev_pi_solr.properties");
    //log记录
    private static final Logger logger = LoggerFactory.getLogger(SolrIndexTools.class);
    //实例化solr的client
    static CloudSolrClient client = null;
    //添加批处理阈值
    static int add_batchCount = config.getInt("add_batchCount");
    //删除的批处理阈值
    static int del_batchCount = config.getInt("del_batchCount");
    //添加的集合缓冲
    static List<SolrInputDocument> add_docs = new ArrayList<SolrInputDocument>();
    //删除的集合缓冲
    static List<String> del_docs = new ArrayList<String>();

    static List<String> zkHosts = new ArrayList<String>();
    // 任何时候，保证只能有一个线程在提交索引，并清空集合
    final static Semaphore semp = new Semaphore(1);

    static {
        logger.info("初始化索引调度...");
        String zk_host = config.getString("zk_host");
        zkHosts = Arrays.asList(zk_host.split(","));
        client = new CloudSolrClient.Builder().withZkHost(zkHosts).build();
        //获取solr collection
        String solr_collection = config.getString("solr_collection");
        client.setDefaultCollection(solr_collection);
        client.setZkClientTimeout(10000);
        client.setZkConnectTimeout(10000);
        //启动定时任务， 第一次延时1s执行， 之后每隔指定时间30s执行一次

        Timer timer = new Timer();
        timer.schedule(new SolrCommit(), config.getInt("first_delay") * 1000, config.getInt("interval_commit_index") * 1000);
    }

    /**
     * 添加数据到临时存储中，如果大于等于batchCount时，就提交一次。
     * 再清空集合，其他情况下走对应的时间间隔提交
     * @param doc
     */
    public static void addDoc(SolrInputDocument doc){
        commitIndex(add_docs, add_batchCount, doc, true);
    }


    /**
     * 删除的数据添加到临时存储中，如果大于对应的批处理就直接提交，再清空集合，
     * 其他情况下走对应的时间间隔提交
     * @param rowkey
     */
    public static void delDoc(String rowkey){
        commitIndex(del_docs, del_batchCount, rowkey, false);
    }

    /**
     * 此方法要加锁，并且提交时，与时间间隔提交是互斥的
     * 百分百确保不会丢失数据
     * @param datas
     * @param count
     * @param doc
     * @param isAdd
     */
    public synchronized static void commitIndex(List datas, int count, Object doc, boolean isAdd) {
        try {
            semp.acquire();//获取信号量
            if (datas.size() >= count) {
                if (isAdd) {
                    client.add(datas);//添加数据到服务器中
                } else {
                    client.deleteById(datas);//删除数据
                }
                client.commit();//提交数据
                datas.clear();//清空临时数据
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("按阀值" + (isAdd == true ? "添加" : "删除") + "操作索引数据出错！ ", e);
        } finally {
            datas.add(doc);//添加单条数据
            semp.release();//释放信号量
        }
    }

    public static class SolrCommit extends TimerTask{
        @Override
        public void run() {
            logger.info("索引线程运行中...");
            //只有等与true时才执行下面的提交代码
            try{
                semp.acquire();//获取信号量
                if (add_docs.size() > 0){
                    client.add(add_docs);//添加
                }
                if (del_docs.size() > 0){
                    client.deleteById(del_docs);//删除
                }
                //确保都有数据提交
                if (add_docs.size() > 0 || del_docs.size() > 0){
                    client.commit();//共用一个提交策略
                    //清空缓冲区的添加和删除数据
                    add_docs.clear();
                    del_docs.clear();
                }else {
                    logger.info("暂无索引数据，跳过commit，继续监听...");
                }
            }catch (Exception e){
                logger.error("间隔提交索引数据出错： " , e);
            }finally {
                semp.release();
            }
        }
    }







}
