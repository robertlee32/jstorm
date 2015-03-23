package com.testin.common.config;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.io.ByteArrayInputStream;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.configuration.*;
import org.apache.log4j.Logger;
import com.github.zkclient.IZkDataListener;
import com.github.zkclient.ZkClient;
import com.github.zkclient.exception.ZkNoNodeException;

/**
 * Composite the XMLConfiguration class from apache com.testin.common configuration component.
 *
 * @author liyi
 */
public class ConfigManager {
    private static Logger logger = Logger.getLogger(ConfigManager.class);
    private static ConfigManager manager = new ConfigManager();
    private XMLConfiguration xmlConfig;
    private Map<String, HierarchicalConfiguration> topolopyMap;
    private Map<String, HierarchicalConfiguration> spoutMap;
    private Map<String, HierarchicalConfiguration> pojoMap;
    //公共资源
    private Map<String, HierarchicalConfiguration> commonResourceMap;
    /**
     * redis资源集合
     */
    private Map<String, HierarchicalConfiguration> redisResourceMap;

    /**
     * mysql资源集合
     */
    private Map<String, HierarchicalConfiguration> mysqlResourceMap;

    /**
     * mongoDB资源集合
     */
    private Map<String, HierarchicalConfiguration> mongoResourceMap;
    private ZkClient zkClient;  // get thunder.xml from remote zookeeper
    private List<ConfigChangedListener> zkChangeSubscriberList;
    private String zk_list;
    private String zk_path;  
    private ConfigManager() {
        String zk_url = System.getProperty("zk.conf");
        if (zk_url==null) {
            logger.warn("can not find system properties \"zk.conf\" property, use default file zk.properties!");
            zk_url = "zk.properties";
        }
      
        //logger.debug("+++ config file  at " + zk_url);  
      
//        String zk_path = "";
        try {        
            PropertiesConfiguration zkConfig = new PropertiesConfiguration(zk_url);
            this.zk_list = zkConfig.getString("zk_server");
            this.zk_path = zkConfig.getString("thunder_zk_path");
            this.zkClient = new ZkClient(zk_list);
            this.zkClient.subscribeDataChanges(zk_path, new ConfigChangedWatcher());
        } catch(ConfigurationException cex) {
            logger.error("failed to load config file " + zk_url , cex);
        }      

        try {        
            byte[] thund_conf_data = this.zkClient.readData(zk_path);
            init(thund_conf_data);     
        } catch (ZkNoNodeException zkEx) {
            logger.error("can not get  thunder.xml from zookeeper", zkEx);  
        }
                
        zkChangeSubscriberList = new ArrayList<ConfigChangedListener> ();
    }
    
    private void init(byte[] data) {
        try {    
            logger.info("get thunder.xml from zk with: \n" + new String(data));
            xmlConfig = new XMLConfiguration();
            xmlConfig.load(new ByteArrayInputStream(data));         
        } catch(ConfigurationException cex) {
            logger.error("failed to prase thunder.xml data stream !" , cex);
        }      
        
        List<HierarchicalConfiguration> topologyList = xmlConfig.configurationsAt("topology");
        topolopyMap = new HashMap<String, HierarchicalConfiguration>(topologyList.size());
        
        for(HierarchicalConfiguration topoConf : topologyList) {
            String topology_id = topoConf.getString("[@id]");
            topolopyMap.put(topology_id, topoConf); 
            //logger.debug("+++++  topo=" + topology_id + "," + topoConf.toString());
        } 

        List<HierarchicalConfiguration> spoutList = xmlConfig.configurationsAt("spout");
        spoutMap = new HashMap<String, HierarchicalConfiguration>(spoutList.size());
        
        for(HierarchicalConfiguration spoutConf : spoutList) {
            String spout_id = spoutConf.getString("[@id]");
            spoutMap.put(spout_id, spoutConf); 
            //logger.debug("+++++  spout=" + spout_id + "," + spoutConf.toString());
        } 

        List<HierarchicalConfiguration> pojoList = xmlConfig.configurationsAt("pojo");
        pojoMap = new HashMap<String, HierarchicalConfiguration>(pojoList.size());
        
        for(HierarchicalConfiguration pojoConf : pojoList) {
            String pojo_id = pojoConf.getString("[@id]");
            pojoMap.put(pojo_id, pojoConf); 
            //logger.debug("+++++  pojo=" + pojo_id + "," + pojoConf.toString());
        }

        //公共资源配置信息
        List<HierarchicalConfiguration> resourceList = xmlConfig.configurationsAt("resource");
        //加载公共资源
        loadCommonResource(resourceList);
    }

    /**
     * 加载公共资源
     * @param resourceList 公共资源列表
     */
    private void loadCommonResource(List<HierarchicalConfiguration> resourceList) {

        if (resourceList == null || resourceList.size() == 0) {
            return;
        }

        for (HierarchicalConfiguration hc : resourceList) {
            //加载redis资源
            List<HierarchicalConfiguration> redisList = hc.configurationsAt("redis-list");
            loadRedisResource(redisList);
            //加载mysql资源
            List<HierarchicalConfiguration> mysqlList = hc.configurationsAt("mysql-list");
            loadMysqlResource(mysqlList);
            //加载mongoDB资源
            List<HierarchicalConfiguration> mongoDBList = hc.configurationsAt("mongo-list");
            loadMongoResource(mongoDBList);

        }
    }

    /**
     * 加载mongoDB资源
     * @param mongoDBList mongoDB资源列表
     */
    private void loadMongoResource(List<HierarchicalConfiguration> mongoDBList) {
        if (mongoDBList == null || mongoDBList.size() == 0) {
            return;
        }

        Map<String,HierarchicalConfiguration> tempMongoResourceMap = new ConcurrentHashMap<>();
        for (HierarchicalConfiguration mongoDB : mongoDBList) {
            List<HierarchicalConfiguration> mongoConfigList = mongoDB.configurationsAt("mongodb");

            if (mongoConfigList == null || mongoConfigList.size() == 0) {
                continue;
            }
            for (HierarchicalConfiguration mongoConfig : mongoConfigList) {
                String mongoId = mongoConfig.getString("[@id]");

                if (tempMongoResourceMap.containsKey(mongoId)) {
                    throw new RuntimeException("发现重复的mongo ID");
                }
                tempMongoResourceMap.put(mongoId, mongoConfig);
            }
        }

        mongoResourceMap = tempMongoResourceMap;
    }

    /**
     * 加载Mysql资源
     * @param mysqlList mysql资源列表
     */
    private void loadMysqlResource(List<HierarchicalConfiguration> mysqlList) {
        if (mysqlList == null || mysqlList.size() == 0) {
            return;
        }

        Map<String,HierarchicalConfiguration> tempMysqlResourceMap = new ConcurrentHashMap<>();
        for (HierarchicalConfiguration mysql : mysqlList) {
            List<HierarchicalConfiguration> mysqlConfigList = mysql.configurationsAt("mysql");

            if (mysqlConfigList == null || mysqlConfigList.size() == 0) {
                continue;
            }
            for (HierarchicalConfiguration mysqlConfig : mysqlConfigList) {
                String mysqlId = mysqlConfig.getString("[@id]");

                if (tempMysqlResourceMap.containsKey(mysqlId)) {
                    throw new RuntimeException("发现重复的mysql ID");
                }
                tempMysqlResourceMap.put(mysqlId, mysqlConfig);
            }
        }
        mysqlResourceMap = tempMysqlResourceMap;
    }

    /**
     * 加载redis资源
     * @param redisResource redis资源列表
     */
    private void loadRedisResource(List<HierarchicalConfiguration> redisResource) {
        if (redisResource == null || redisResource.size() == 0) {
            return;
        }

        Map<String,HierarchicalConfiguration> tempRedisResourceMap = new ConcurrentHashMap<>();
        for (HierarchicalConfiguration redis : redisResource) {
            List<HierarchicalConfiguration> redisConfigList = redis.configurationsAt("redis");

            if (redisConfigList == null || redisConfigList.size() == 0) {
                continue;
            }
            for (HierarchicalConfiguration redisConfig : redisConfigList) {
                String redisId = redisConfig.getString("[@id]");

                if (tempRedisResourceMap.containsKey(redisId)) {
                    throw new RuntimeException("发现重复的redis ID");
                }
                tempRedisResourceMap.put(redisId, redisConfig);
            }
        }

        redisResourceMap = tempRedisResourceMap;
    }

    public static  ConfigManager getInstance() {
        return manager;
    }
       
    public HierarchicalConfiguration getConfig() {
        return xmlConfig; 
    }   
    
    public HierarchicalConfiguration getTopologyConfig(String topologyID) {
        return topolopyMap.get(topologyID);
    }

    public HierarchicalConfiguration getSpoutConfig(String spoutID) {
        return spoutMap.get(spoutID);
    }

    public HierarchicalConfiguration getPojoConfig(String pojoID) {
        return pojoMap.get(pojoID);
    }

    /**
     * 获取redis标识的相关配置
     * @param redisId redis ID
     * @return redis配置，如果不存在返回<code>null</code>
     */
    public HierarchicalConfiguration getRedisConfig(String redisId) {
        if (redisResourceMap == null) {
            return null;
        }
        return redisResourceMap.get(redisId);
    }

    /**
     * 获取mysql标识的相关配置
     * @param mysqlId mysql ID
     * @return mysql配置，如果不存在返回<code>null</code>
     */
    public HierarchicalConfiguration getMysqlConfig(String mysqlId) {
        if (mysqlResourceMap == null) {
            return null;
        }
        return mysqlResourceMap.get(mysqlId);
    }

    /**
     * 获取mongo标识的相关配置
     * @param mongoId mongo ID
     * @return mongo配置，如果不存在返回<code>null</code>
     */
    public HierarchicalConfiguration getMongoConfig(String mongoId) {
        if (mongoResourceMap == null) {
            System.out.println("mongo error ........");
            return null;
        }
        return mongoResourceMap.get(mongoId);
    }

    public void subscribe(ConfigChangedListener listener) {
        zkChangeSubscriberList.add(listener);         
    }
    
    public String getZKPath() {
        return this.zk_path;
    }
    
    private class ConfigChangedWatcher implements IZkDataListener {
        public void handleDataChange(String dataPath, byte[] data) throws Exception {
            /*if (dataPath == null || !dataPath.equals(getZKPath())) {
                return ;
            }*/

            if (data == null || data.length <= 0) {
            	return;
            }
                        
            logger.info("ConfigManger recieve zk data. reloading thunder.xml");
            init(data);
            
            // notify subscribers
            logger.info("Notify subscribers zk changed.");
            if (!zkChangeSubscriberList.isEmpty()) {
                for(ConfigChangedListener subscriber : zkChangeSubscriberList) {
                    subscriber.configureChanged();
                }
            }
        }
        
        public void handleDataDeleted(String dataPath) throws Exception {
          // do nothing;      
        }
    }
}
