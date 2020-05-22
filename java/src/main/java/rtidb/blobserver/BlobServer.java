package rtidb.blobserver;

import com._4paradigm.rtidb.blobserver.OSS;

public interface BlobServer {
    OSS.PutResponse put(OSS.PutRequest request);
    OSS.GetResponse get(OSS.GetRequest reuqest);
    OSS.DeleteResponse delete(OSS.DeleteRequest request);
}
