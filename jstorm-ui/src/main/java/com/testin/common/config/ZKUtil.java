package com.testin.common.config;

import com.github.zkclient.IZkClient;
import com.github.zkclient.ZkClient;

import java.io.*;
import java.util.Properties;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;


/**
 * 配置信息获取类
 *
 * @author rollinkin
 * @date 2015-03-06
 */
public class ZKUtil {

    //private static final Logger LOGGER = LoggerFactory.getLogger(ZKUtil.class);

    /**
     * 根据配置Key获得zk的相应node路径
     *
     * @param configureKey config Key
     * @return node路径
     */
    private static String getZkNode(String configureKey) {
        return "/com/testin/configure/" + configureKey;
    }

    /**
     * 获取默认的zk node路径
     *
     * @return node路径
     */
    private static String getZkNode() {
        return getZkNode("default");
    }

    private static boolean isEmpty(String str) {
        return str == null || "".equals(str);
    }

    /**
     * 上传系统配置文件到zk系统中保存
     *
     * @param zkConfigFilePath zk集群的配置文件路径
     * @param configFilePath   真实的配置文件路径
     */
    public static void uploadConfigureZk(String zkConfigFilePath, String configFilePath) throws IOException {
        if (isEmpty(zkConfigFilePath)) {
            throw new RuntimeException("zkConfigFilePath can't null");
        }

        if (isEmpty(configFilePath)) {
            throw new RuntimeException("configFilePath can't null");
        }

        //读取zk的配置信息
        Properties zkProperties = readProperties(zkConfigFilePath);

        //zk服务器列表
        String zkServerList = zkProperties.getProperty("zk_server");
        //读取配置信息
        String configInfo = readConfigureByFile(configFilePath);

        //连接到zk，获取配置信息
        IZkClient zkClient = null;
        try {
            zkClient = new ZkClient(zkServerList);
            String nodePath = getZkNode();
            if (zkClient.exists(nodePath)) {
                zkClient.delete(nodePath);
            } 
            zkClient.createPersistent(nodePath, true);

            zkClient.writeData(nodePath, configInfo.getBytes());
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
    }

    /**
     * 读取ZK的配置文件
     *
     * @param zkConfigPath zk配置文件路径
     * @return zk配置信息
     * @throws java.io.IOException
     */
    private static Properties readProperties(String zkConfigPath) throws IOException {
        File zkConfigFile = new File(zkConfigPath);
        InputStream zkInputStream = null;
        Properties properties = new Properties();

        try {
            if (zkConfigFile.exists()) {
                zkInputStream = new FileInputStream(zkConfigFile);
            } else {
                zkInputStream = ZKUtil.class.getResourceAsStream("/" + zkConfigPath);
            }

            properties.load(zkInputStream);
        } finally {
            if (zkInputStream != null) {
                zkInputStream.close();
            }
        }
        return properties;
    }

    /**
     * 读取指定的配置信息
     *
     * @param configureFilePath 配置文件路径
     * @return 文件内容
     * @throws java.io.IOException
     */
    private static String readConfigureByFile(String configureFilePath) throws IOException {
        File configureFile = new File(configureFilePath);
        InputStream inputStream = null;
        //从文件中读取配置信息
        //StringBuilder configInfo = new StringBuilder();
        StringBuffer  configInfo = new StringBuffer();
        try {

            if (configureFile.exists()) {
                inputStream = new FileInputStream(configureFile);
            } else {
                inputStream = ZKUtil.class.getResourceAsStream("/" + configureFilePath);
            }

            BufferedReader br=new BufferedReader(new InputStreamReader(inputStream));
						String line="";
						while((line=br.readLine())!=null){
							configInfo.append(line);
						}
						
						
            /*byte[] tempBytes = new byte[1024];
            while (inputStream.read(tempBytes) != -1) {
                configInfo.append(new String(tempBytes));
                tempBytes = new byte[1024];
            }*/
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }

        return configInfo.toString();
    }

    public static void main(String[] args)  {
        if (args.length < 2) {
            printHelp();
        }
      
        File file = new File(args[0]);
        if (!file.exists()) {
           System.out.println("zk.properties at \"" + file + "\" not exists!");   
            System.exit(0);
        }

        file = new File(args[1]);
        if (!file.exists()) {
           System.out.println("thunder.xml at \"" + file + "\" not exists!");   
            System.exit(0);
        }
        
        try {
            ZKUtil.uploadConfigureZk(args[0], args[1]);
        } catch (IOException ioEx) {
           System.out.println("Failed to update thunder.xml to zookeeper!");
           ioEx.printStackTrace();   
        }
        
        System.out.println("Up load thunder.xml to zookeeper successed! ");
        System.exit(0);                        
    }
    
    public static void printHelp() {
        String help = "usage: java -cp thunder.jar  com.testin.thunder.util.ZKUtile zk.properties thunder.xml \n" +
                     "\n" +
                     " zk.properties : the properties file tells the location of zookeeper cluster  \n" +
                     " thunder.xml : the configuration file for the thunder project ";
        System.out.println(help);              
   }
}
