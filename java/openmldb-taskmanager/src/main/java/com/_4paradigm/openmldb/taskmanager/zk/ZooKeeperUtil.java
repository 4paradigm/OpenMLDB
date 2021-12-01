package com._4paradigm.openmldb.taskmanager.zk;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Most code is from HBase ZKUtil and ZooKeeperWatcher is replaced with FailoverWatcher.
 */
public class ZooKeeperUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperUtil.class);

  public static boolean watchAndCheckExists(FailoverWatcher failoverWatcher, String znode)
      throws KeeperException {
    try {
      Stat s = failoverWatcher.getZooKeeper().exists(znode, failoverWatcher);
      boolean exists = s != null;
      if (LOG.isDebugEnabled()) {
        if (exists) {
          LOG.debug("Set watcher on existing znode " + znode);
        } else {
          LOG.debug(znode + " does not exist. Watcher is set.");
        }
      }
      return exists;
    } catch (KeeperException e) {
      LOG.warn("Unable to set watcher on znode " + znode, e);
      LOG.warn("Received unexpected KeeperException, re-throwing exception");
      throw e;
    } catch (InterruptedException e) {
      LOG.warn("Unable to set watcher on znode " + znode, e);
      return false;
    }
  }

  public static boolean createEphemeralNodeAndWatch(FailoverWatcher failoverWatcher, String znode,
      byte[] data) throws KeeperException {
    try {
      LOG.info("Try to create emphemeral znode " + znode);
      failoverWatcher.getZooKeeper().create(znode, data, createAcl(failoverWatcher, znode), CreateMode.EPHEMERAL);
    } catch (KeeperException.NodeExistsException nee) {
      if (!watchAndCheckExists(failoverWatcher, znode)) {
        // It did exist but now it doesn't, try again
        return createEphemeralNodeAndWatch(failoverWatcher, znode, data);
      }
      return false;
    } catch (InterruptedException e) {
      LOG.info("Interrupted", e);
      Thread.currentThread().interrupt();
    }
    return true;
  }

  public static void deleteNode(FailoverWatcher failoverWatcher, String node)
      throws KeeperException {
    deleteNode(failoverWatcher, node, -1);
  }

  public static boolean deleteNode(FailoverWatcher failoverWatcher, String node, int version)
      throws KeeperException {
    try {
      failoverWatcher.getZooKeeper().delete(node, version);
      return true;
    } catch (KeeperException.BadVersionException bve) {
      LOG.debug("Bad version exception when delete node '{}'", node, bve);
      return false;
    } catch (InterruptedException ie) {
      LOG.debug("Received InterruptedException, doing nothing here", ie);
      return false;
    }
  }
  
  public static void deleteNodeRecursively(FailoverWatcher failoverWatcher, String node)
  throws KeeperException {
    try {
      List<String> children = ZooKeeperUtil.listChildrenNoWatch(failoverWatcher, node);
      // the node is already deleted, so we just finish
      if (children == null) {
        return;
      }

      if(!children.isEmpty()) {
        for(String child : children) {
          deleteNodeRecursively(failoverWatcher, node + "/" + child);
        }
      }
      failoverWatcher.getZooKeeper().delete(node, -1);
    } catch(InterruptedException ie) {
      LOG.debug("Receive InterruptedException, doing nothing here", ie);
    }
  }
  
  public static List<String> listChildrenNoWatch(FailoverWatcher failoverWatcher, String znode)
      throws KeeperException {
    List<String> children = null;
    try {
      // List the children without watching
      children = failoverWatcher.getZooKeeper().getChildren(znode, null);
    } catch (KeeperException.NoNodeException nne) {
      LOG.debug("child '{}' does not exist, we ignore the result:", znode, nne);
    } catch (InterruptedException ie) {
      LOG.debug("Receive InterruptedException, doing nothing here", ie);
    }

    return children;
  }

  public static void deleteNodeFailSilent(FailoverWatcher failoverWatcher, String node)
      throws KeeperException {
    try {
      failoverWatcher.getZooKeeper().delete(node, -1);
    } catch (KeeperException.NoNodeException ignored) {
      //ignore
    } catch (InterruptedException ie) {
      LOG.debug("Received InterruptedException, doing nothing here", ie);
    }
  }

  public static byte[] getDataAndWatch(FailoverWatcher failoverWatcher, String znode) {
    return getDataInternal(failoverWatcher, znode, null);
  }

  @Nullable
  private static byte[] getDataInternal(FailoverWatcher failoverWatcher, String znode, Stat stat) {
    try {
      byte[] data = failoverWatcher.getZooKeeper().getData(znode, failoverWatcher, stat);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Retrieved " + ((data == null) ? 0 : data.length)
            + " byte(s) of data from znode " + znode);
      }
      return data;
    } catch (KeeperException.NoNodeException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Unable to get data of znode " + znode + " "
            + "because node does not exist (not an error)");
      }
      return null;
    } catch (KeeperException | InterruptedException e) {
      LOG.warn("Unable to get data of znode " + znode, e);
      LOG.warn("Received unexpected KeeperException, re-throwing exception");
      return null;
    }
  }

  public static void createAndFailSilent(FailoverWatcher failoverWatcher, String znode)
      throws KeeperException, InterruptedException {
    try {
      LOG.info("Try to create persistent znode " + znode);
      ZooKeeper zk = failoverWatcher.getZooKeeper();
      if (zk.exists(znode, false) == null) {
        zk.create(znode, new byte[0], createAcl(failoverWatcher, znode), CreateMode.PERSISTENT);
      }
    } catch (KeeperException.NodeExistsException ignore) {
      //we just ignore result if the node already exist
      LOG.info("Znode " + znode + " already exist");
    } catch (KeeperException.NoAuthException nee) {
      try {
        if (null == failoverWatcher.getZooKeeper().exists(znode, false)) {
          // If we failed to create the file and it does not already exist.
          throw nee;
        }
      } catch (InterruptedException ie) {
        LOG.debug("Received InterruptedException, re-throw the exception", ie);
        throw ie;
      }
    } catch (InterruptedException ie) {
      LOG.debug("Received InterruptedException, re-throw the exception", ie);
      throw ie;
    }
  }

  public static void setData(FailoverWatcher failoverWatcher, String znode, byte[] data)
      throws KeeperException {
    setData(failoverWatcher, znode, data, -1);
  }

  public static boolean setData(FailoverWatcher failoverWatcher, String znode, byte[] data,
      int expectedVersion) throws KeeperException {
    try {
      return failoverWatcher.getZooKeeper().setData(znode, data, expectedVersion) != null;
    } catch (InterruptedException e) {
      LOG.debug("Received InterruptedException, doing nothing here", e);
      return false;
    }
  }

  public static List<String> listChildrenAndWatchForNewChildren(FailoverWatcher failoverWatcher,
      String znode) {
    try {
      return failoverWatcher.getZooKeeper().getChildren(znode, failoverWatcher);
    } catch (KeeperException.NoNodeException ke) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Unable to list children of znode " + znode + " "
            + "because node does not exist (not an error)");
      }
      return null;
    } catch (KeeperException e) {
      LOG.warn("Unable to list children of znode " + znode + " ", e);
      LOG.warn("Received unexpected KeeperException, re-throwing exception");
      return null;
    } catch (InterruptedException e) {
      LOG.warn("Unable to list children of znode " + znode + " ", e);
      LOG.warn("Received InterruptedException, doing nothing here");
      return null;
    }
  }

  /**
   * Create acl for znodes, anyone could read, but only admin can operate if set scure.
   *
   * @param failoverWatcher the watcher which has the configuration
   * @return the acls
   */
  public static ArrayList<ACL> createAcl(FailoverWatcher failoverWatcher, String znode) {
    if ("/chronos".equals(znode)) {
      return Ids.OPEN_ACL_UNSAFE;
    }

    /*
    if (failoverWatcher.isZkSecure()) {
      ArrayList<ACL> acls = new ArrayList<>();
      acls.add(new ACL(ZooDefs.Perms.READ, Ids.ANYONE_ID_UNSAFE));
      acls.add(new ACL(ZooDefs.Perms.ALL, new Id("sasl", failoverWatcher.getZkAdmin())));
      return acls;
    }
    */

    return Ids.OPEN_ACL_UNSAFE;
  }

  /**
   * Convert long value to byte array.
   *
   * @param val the long value
   * @return the byte array of this value
   */
  public static byte[] longToBytes(long val) {
    byte[] b = new byte[Byte.SIZE];
    for (int i = 7; i > 0; i--) {
      b[i] = (byte) val;
      val >>>= 8;
    }
    b[0] = (byte) val;
    return b;
  }

  /**
   * Convert byte array to original long value.
   *
   * @param bytes the byte array
   * @return the original long value
   */
  public static long bytesToLong(byte[] bytes) {
    long l = 0;
    for (int i = 0; i < Byte.SIZE; i++) {
      l <<= 8;
      l ^= bytes[i] & 0xFF;
    }
    return l;
  }

}
