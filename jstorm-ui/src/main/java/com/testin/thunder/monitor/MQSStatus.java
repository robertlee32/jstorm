package com.testin.thunder.monitor;

import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import com.aliyun.mqs.client.CloudQueue;
import com.aliyun.mqs.client.DefaultMQSClient;
import com.aliyun.mqs.client.MQSClient;
import com.aliyun.mqs.client.CloudQueue;
import com.aliyun.mqs.common.ServiceException;
import com.aliyun.mqs.model.Message;
import com.aliyun.mqs.model.QueueMeta;
import com.testin.common.config.ConfigManager;
import org.apache.commons.configuration.HierarchicalConfiguration;

/**
 * @author liyi
 * 
 */ 
public class MQSStatus extends HttpServlet {
    private static Logger logger = Logger.getLogger(MQSStatus.class);
    private static final long serialVersionUID = -2128122331111219481L;
    
    public void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        String spoutID = req.getParameter("id");
        String result = "";
        
        if(spoutID == null || spoutID.length() <= 0) {
            printOut("miss id", res);
            return;
        }
        
        HierarchicalConfiguration spoutConf = ConfigManager.getInstance().getSpoutConfig(spoutID);
        if(spoutConf == null ) {
            printOut("unkown spout id", res);
            return;
        }
    
        String mqs_url = spoutConf.getString("MQS.url");
        String mqs_key = spoutConf.getString("MQS.key");
        String mqs_sec = spoutConf.getString("MQS.secret");
        String mqs_queue_name = spoutConf.getString("MQS.queuename");
        
        String msg = "mqs_url:" + mqs_url + ",mqs_key:" + mqs_key + ",mqs_sec:"+mqs_sec + ",mqs_queue_name:" + mqs_queue_name ;
        logger.info(msg);
        
        MQSClient client = new DefaultMQSClient(mqs_url, mqs_key, mqs_sec);
        try{
            CloudQueue queue = client.getQueueRef(mqs_queue_name);
            
            QueueMeta meta = queue.getAttributes();
            result = "{\"queuename\":\"" + mqs_queue_name + "\", \"activeMessages\":\"" + meta.getActiveMessages() + "\", \"delayMessages:\"" + 
                    meta.getDelayMessages() + "\", \"inactiveMessages\":\"" + meta.getInactiveMessages() + "\", \"maxMessageSize\":\"" + 
                    meta.getMaxMessageSize() + "\"}"; 
        } catch (ServiceException ex) {
            logger.error("failed to query mqs=" + mqs_queue_name , ex);
            result = ex.getMessage();
        }
        
        printOut(result, res);
    }
  
    public void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
      doGet(req, res);
    }
    
    private void printOut(String msg, HttpServletResponse res) throws IOException {
          PrintWriter out = res.getWriter();
          out.println(msg);
          out.close();
    }
}



