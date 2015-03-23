package com.testin.thunder.monitor;

import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
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
import com.alibaba.jstorm.ui.model.data.*;
import com.alibaba.jstorm.ui.model.*;

/**
 * @author liyi
 * 
 */ 
public class TopologyStatus extends HttpServlet {
    private static Logger logger = Logger.getLogger(TopologyStatus.class);
    private static final long serialVersionUID = -2128122331111219481L;
    private static final String window = "600";
    public void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        String spoutID = req.getParameter("id");
        String result = "";
        
        if(spoutID == null || spoutID.length() <= 0) {
            printOut("miss id", res);
            return;
        }
        
        String statStr = "no data";
        
        try {
            com.testin.thunder.monitor.MainPage mainPage = new MainPage();
            List<TopologySumm> topoList = mainPage.getTsumm();
            logger.info("+++ manPage:" + mainPage);
            for(TopologySumm topoSumm : topoList) {
                String topologyName = topoSumm.getTopologyName();
                String topologyID = topoSumm.getTopologyId();
                logger.info("+++ topoPage:" + topologyName + ", id=" +topologyID );
                TopologyPage topoPage = new TopologyPage(null, topologyID, window);
                List<WinComponentStats> stats =  topoPage.getTstats();
                String winStr="";
                for(WinComponentStats stat : stats) {
                    winStr = "{Window:" + stat.getWindow() + ",Emitted:" + stat.getEmitted() + ",Send_TPS:" +
                                stat.getSendTps() + ",Recv_TPS:" + stat.getRecvTps() + ",acked:" + 
                                stat.getAcked() + "}";    
                    logger.info("win:" +winStr);
                }
                
                String spoutStr="";
                List<Components> spoutStats = topoPage.getScom();
                for(Components scom : spoutStats) {
                    spoutStr = "{SpoutId:" + scom.getComponetId() + ",Emitted:" + scom.getEmitted() + ",Send_TPS:" +
                                scom.getSendTps() + ",Recv_TPS:" + scom.getRecvTps() + ",acked:" + 
                                scom.getAcked() + "}";    
                    logger.info("win:" +spoutStr);
                }
                
                StringBuffer boltStr = new StringBuffer();
                boltStr.append("{[");
                List<Components> boltStats = topoPage.getBcom();
                
                for(Components bcom : boltStats) {
                    String str = "{BoltId:" + bcom.getComponetId() + ",Emitted:" + bcom.getEmitted() + ",Send_TPS:" +
                                bcom.getSendTps() + ",Recv_TPS:" + bcom.getRecvTps() + ",acked:" + 
                                bcom.getAcked() + "},";    
                    logger.info("win:" + str);
                    boltStr.append(str);
                }     
                
                boltStr.append("]}");
                
                statStr = "{topology:" + winStr + ",spout:" + spoutStr + ",bolt:" + boltStr.toString() + "}";
                logger.info("+++ stat:" + statStr);           
            }        
            
        } catch (Exception ex) {
            logger.error("get topo error", ex);        
        }

        printOut(statStr, res);

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



