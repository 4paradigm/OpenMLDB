package com._4paradigm.rtidb.utils;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IpAddressUtils {
    private final static Logger logger = LoggerFactory.getLogger(IpAddressUtils.class);
    public static boolean isLocalHost(String ip) {
        try {
            InetAddress addr = InetAddress.getByName(ip);
            if (addr.isAnyLocalAddress() || addr.isLoopbackAddress()) {
                return true;
            }
            return NetworkInterface.getByInetAddress(addr) != null;
        } catch (UnknownHostException e) {
            logger.error("fail to get ip info {}", ip, e);
            return false;
        } catch (SocketException e) {
            logger.error("fail to get ip info {}", ip, e);
            return false;
        }
    }
}
