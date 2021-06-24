package com._4paradigm.dataimporter;

import org.apache.log4j.Logger;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.fs.CommonConfigurationKeys;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;
import java.util.Map;
import java.util.Random;

public class FileSystemManager {
    private static final Logger logger = Logger.getLogger(FileSystemManager.class);

    // supported scheme
    private static final String HDFS_SCHEME = "hdfs";
    private static final String USER_NAME_KEY = "username";
    private static final String PASSWORD_KEY = "password";
    private static final String AUTHENTICATION_SIMPLE = "simple";
    private static final String AUTHENTICATION_KERBEROS = "kerberos";
    private static final String KERBEROS_PRINCIPAL = "kerberos_principal";
    private static final String KERBEROS_KEYTAB = "kerberos_keytab";
    private static final String KERBEROS_KEYTAB_CONTENT = "kerberos_keytab_content";
    private static final String DFS_HA_NAMENODE_KERBEROS_PRINCIPAL_PATTERN =
            "dfs.namenode.kerberos.principal.pattern";
    // arguments for ha hdfs
    private static final String DFS_NAMESERVICES_KEY = "dfs.nameservices";
    private static final String DFS_HA_NAMENODES_PREFIX = "dfs.ha.namenodes.";
    private static final String DFS_HA_NAMENODE_RPC_ADDRESS_PREFIX = "dfs.namenode.rpc-address.";
    private static final String DFS_CLIENT_FAILOVER_PROXY_PROVIDER_PREFIX =
            "dfs.client.failover.proxy.provider.";
    private static final String DEFAULT_DFS_CLIENT_FAILOVER_PROXY_PROVIDER =
            "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider";
    private static final String FS_DEFAULTFS_KEY = "fs.defaultFS";
    // If this property is not set to "true", FileSystem instance will be returned from cache
    // which is not thread-safe and may cause 'Filesystem closed' exception when it is closed by other thread.
    private static final String FS_HDFS_IMPL_DISABLE_CACHE = "fs.hdfs.impl.disable.cache";

    // TODO hdfs path & config => FileSystem
    // how to use filesystem is the next stage
    /**
     * visible for test
     *
     * @param path
     * @param properties
     * @return BrokerFileSystem with different FileSystem based on scheme
     * @throws URISyntaxException
     * @throws Exception
     */
//    public BrokerFileSystem getFileSystem(String path, Map<String, String> properties) {
//        WildcardURI pathUri = new WildcardURI(path);
//        String scheme = pathUri.getUri().getScheme();
//        if (Strings.isNullOrEmpty(scheme)) {
//            throw new BrokerException(TBrokerOperationStatusCode.INVALID_INPUT_FILE_PATH,
//                    "invalid path. scheme is null");
//        }
//        BrokerFileSystem brokerFileSystem = null;
//        if (scheme.equals(HDFS_SCHEME)) {
//            brokerFileSystem = getDistributedFileSystem(path, properties);
//        } else if (scheme.equals(S3A_SCHEME)) {
//            brokerFileSystem = getS3AFileSystem(path, properties);
//        } else {
//            throw new BrokerException(TBrokerOperationStatusCode.INVALID_INPUT_FILE_PATH,
//                    "invalid path. scheme is not supported");
//        }
//        return brokerFileSystem;
//    }

    /**
     * file system handle is cached, the identity is host + username_password
     * it will have safety problem if only hostname is used because one user may specify username and password
     * and then access hdfs, another user may not specify username and password but could also access data
     *
     * @param path
     * @param properties
     * @return
     * @throws Exception
     */
    public void getDistributedFileSystem(String path, Map<String, String> properties) throws IOException, InterruptedException {
        WildcardURI pathUri = new WildcardURI(path);
        String host = HDFS_SCHEME + "://" + pathUri.getAuthority(); // for cache id
        if (Strings.isNullOrEmpty(pathUri.getAuthority())) {
            if (properties.containsKey(FS_DEFAULTFS_KEY)) {
                host = properties.get(FS_DEFAULTFS_KEY);
                logger.info("no schema and authority in path. use fs.defaultFs");
            } else {
                logger.warn("invalid hdfs path. authority is null,path:" + path);
//                throw new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
//                        "invalid hdfs path. authority is null");
            }
        }

        String authentication = properties.getOrDefault(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
                AUTHENTICATION_SIMPLE);
        if (Strings.isNullOrEmpty(authentication) || (!authentication.equals(AUTHENTICATION_SIMPLE)
                && !authentication.equals(AUTHENTICATION_KERBEROS))) {
            logger.warn("invalid authentication:" + authentication);
//            throw new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
//                    "invalid authentication:" + authentication);
        }

        // TODO cache
//        String hdfsUgi = username + "," + password;
//        FileSystemIdentity fileSystemIdentity = null;
//        BrokerFileSystem fileSystem = null;
//        if (authentication.equals(AUTHENTICATION_SIMPLE)) {
//            fileSystemIdentity = new FileSystemIdentity(host, hdfsUgi);
//        } else {
//            // for kerberos, use host + principal + keytab as filesystemindentity
//            String kerberosContent = "";
//            if (properties.containsKey(KERBEROS_KEYTAB)) {
//                kerberosContent = properties.get(KERBEROS_KEYTAB);
//            } else if (properties.containsKey(KERBEROS_KEYTAB_CONTENT)) {
//                kerberosContent = properties.get(KERBEROS_KEYTAB_CONTENT);
//            } else {
//                throw new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
//                        "keytab is required for kerberos authentication");
//            }
//            if (!properties.containsKey(KERBEROS_PRINCIPAL)) {
//                throw new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
//                        "principal is required for kerberos authentication");
//            } else {
//                kerberosContent = kerberosContent + properties.get(KERBEROS_PRINCIPAL);
//            }
//            try {
//                MessageDigest digest = MessageDigest.getInstance("md5");
//                byte[] result = digest.digest(kerberosContent.getBytes());
//                String kerberosUgi = new String(result);
//                fileSystemIdentity = new FileSystemIdentity(host, kerberosUgi);
//            } catch (NoSuchAlgorithmException e) {
//                throw new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
//                        e.getMessage());
//            }
//        }
//        cachedFileSystem.putIfAbsent(fileSystemIdentity, new BrokerFileSystem(fileSystemIdentity));
//        fileSystem = cachedFileSystem.get(fileSystemIdentity);
//        if (fileSystem == null) {
//            // it means it is removed concurrently by checker thread
//            return null;
//        }
//        fileSystem.getLock().lock();
//        try {
//            if (!cachedFileSystem.containsKey(fileSystemIdentity)) {
//                // this means the file system is closed by file system checker thread
//                // it is a corner case
//                return null;
//            }
//            if (fileSystem.getDFSFileSystem() == null) {
//                logger.info("could not find file system for path " + path + " create a new one");
        // create a new filesystem

    }

    private static String preparePrincipal(String originalPrincipal) throws UnknownHostException {
        String finalPrincipal = originalPrincipal;
        String[] components = originalPrincipal.split("[/@]");
        if (components.length == 3) {
            if (components[1].equals("_HOST")) {
                // Convert hostname(fqdn) to lower case according to SecurityUtil.getServerPrincipal
                finalPrincipal = components[0] + "/" +
                        StringUtils.toLowerCase(InetAddress.getLocalHost().getCanonicalHostName())
                        + "@" + components[2];
            } else if (components[1].equals("_IP")) {
                finalPrincipal = components[0] + "/" +
                        InetAddress.getByName(InetAddress.getLocalHost().getCanonicalHostName()).getHostAddress()
                        + "@" + components[2];
            }
        }
        return finalPrincipal;
    }

    public FileSystem createFileSystem(WildcardURI pathUri, Map<String, String> properties) throws IOException, InterruptedException {
        Configuration conf = new Configuration();

        // fallback when kerberos auth fail
        conf.set(CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY, "true");

        // TODO get this param from properties
        // conf.set("dfs.replication", "2");
        String tmpFilePath = null;
        String authentication = properties.getOrDefault(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
                AUTHENTICATION_SIMPLE);
        if (authentication.equals(AUTHENTICATION_KERBEROS)) {
            conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
                    AUTHENTICATION_KERBEROS);

            String principal = preparePrincipal(properties.get(KERBEROS_PRINCIPAL));
            String keytab = "";
            if (properties.containsKey(KERBEROS_KEYTAB)) {
                keytab = properties.get(KERBEROS_KEYTAB);
            } else if (properties.containsKey(KERBEROS_KEYTAB_CONTENT)) {
                // pass kerberos keytab content use base64 encoding
                // so decode it and write it to tmp path under /tmp
                // because ugi api only accept a local path as argument
                String keytab_content = properties.get(KERBEROS_KEYTAB_CONTENT);
                byte[] base64decodedBytes = Base64.getDecoder().decode(keytab_content);
                long currentTime = System.currentTimeMillis();
                Random random = new Random(currentTime);
                int randNumber = random.nextInt(10000);
                // different kerberos account has different file
                tmpFilePath = "/tmp/." + principal + "_" + currentTime + "_" + randNumber;
                FileOutputStream fileOutputStream = new FileOutputStream(tmpFilePath);
                fileOutputStream.write(base64decodedBytes);
                fileOutputStream.close();
                keytab = tmpFilePath;
            } else {
//                        throw  new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
//                                "keytab is required for kerberos authentication");
            }
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
            if (properties.containsKey(KERBEROS_KEYTAB_CONTENT)) {
                try {
                    File file = new File(tmpFilePath);
                    if (!file.delete()) {
                        logger.warn("delete tmp file:" + tmpFilePath + " failed");
                    }
                } catch (Exception e) {
//                    throw new BrokerException(TBrokerOperationStatusCode.FILE_NOT_FOUND,
//                            e.getMessage());
                }
            }
        }

        String dfsNameServices = properties.getOrDefault(DFS_NAMESERVICES_KEY, "");
        if (!Strings.isNullOrEmpty(dfsNameServices)) {
            // ha hdfs arguments
            final String dfsHaNameNodesKey = DFS_HA_NAMENODES_PREFIX + dfsNameServices;
            if (!properties.containsKey(dfsHaNameNodesKey)) {
//                throw new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
//                        "load request missed necessary arguments for ha mode");
            }
            String dfsHaNameNodes = properties.get(dfsHaNameNodesKey);
            conf.set(DFS_NAMESERVICES_KEY, dfsNameServices);
            conf.set(dfsHaNameNodesKey, dfsHaNameNodes);
            String[] nameNodes = dfsHaNameNodes.split(",");
            if (nameNodes == null) {
//                throw new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
//                        "invalid " + dfsHaNameNodesKey + " configuration");
            } else {
                for (String nameNode : nameNodes) {
                    nameNode = nameNode.trim();
                    String nameNodeRpcAddress =
                            DFS_HA_NAMENODE_RPC_ADDRESS_PREFIX + dfsNameServices + "." + nameNode;
                    if (!properties.containsKey(nameNodeRpcAddress)) {
//                        throw new BrokerException(TBrokerOperationStatusCode.INVALID_ARGUMENT,
//                                "missed " + nameNodeRpcAddress + " configuration");
                    } else {
                        conf.set(nameNodeRpcAddress, properties.get(nameNodeRpcAddress));
                    }
                }
            }

            final String dfsClientFailoverProxyProviderKey =
                    DFS_CLIENT_FAILOVER_PROXY_PROVIDER_PREFIX + dfsNameServices;
            if (properties.containsKey(dfsClientFailoverProxyProviderKey)) {
                conf.set(dfsClientFailoverProxyProviderKey,
                        properties.get(dfsClientFailoverProxyProviderKey));
            } else {
                conf.set(dfsClientFailoverProxyProviderKey,
                        DEFAULT_DFS_CLIENT_FAILOVER_PROXY_PROVIDER);
            }
            if (properties.containsKey(FS_DEFAULTFS_KEY)) {
                conf.set(FS_DEFAULTFS_KEY, properties.get(FS_DEFAULTFS_KEY));
            }
            if (properties.containsKey(DFS_HA_NAMENODE_KERBEROS_PRINCIPAL_PATTERN)) {
                conf.set(DFS_HA_NAMENODE_KERBEROS_PRINCIPAL_PATTERN,
                        properties.get(DFS_HA_NAMENODE_KERBEROS_PRINCIPAL_PATTERN));
            }
        }

        conf.set(FS_HDFS_IMPL_DISABLE_CACHE, "true");
        FileSystem dfsFileSystem = null;
        String username = properties.getOrDefault(USER_NAME_KEY, "");
        String password = properties.getOrDefault(PASSWORD_KEY, "");

        if (authentication.equals(AUTHENTICATION_SIMPLE) &&
                properties.containsKey(USER_NAME_KEY) && !Strings.isNullOrEmpty(username)) {
            // Use the specified 'username' as the login name
            UserGroupInformation ugi = UserGroupInformation.createRemoteUser(username);
            // make sure hadoop client know what auth method would be used now,
            // don't set as default
            conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, AUTHENTICATION_SIMPLE);
            ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.SIMPLE);

            dfsFileSystem = ugi.doAs((PrivilegedExceptionAction<FileSystem>) () -> FileSystem.get(pathUri.getUri(), conf));
        } else {
            dfsFileSystem = FileSystem.get(pathUri.getUri(), conf);
        }
        return dfsFileSystem;
    }

}
