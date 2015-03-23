/**
 *
 */
package com.github.zkclient;

import java.util.List;

import org.apache.zookeeper.Watcher.Event.KeeperState;

/**
 * An abstract class for zookeeper listner
 * @author adyliu (imxylz@gmail.com)
 * @since 2012-12-4
 * @see com.github.zkclient.IZkChildListener
 * @see com.github.zkclient.IZkDataListener
 * @see com.github.zkclient.IZkStateListener
 */
public abstract class AbstractListener implements IZkChildListener, IZkDataListener, IZkStateListener {

    @Override
    public void handleStateChanged(KeeperState state) throws Exception {
    }

    @Override
    public void handleNewSession() throws Exception {
    }

    @Override
    public void handleDataChange(String dataPath, byte[] data) throws Exception {
    }

    @Override
    public void handleDataDeleted(String dataPath) throws Exception {
    }

    @Override
    public void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
    }

}
