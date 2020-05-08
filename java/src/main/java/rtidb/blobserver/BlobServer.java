package rtidb.blobserver;

import com._4paradigm.rtidb.object_store.oss;

public interface BlobServer {
    oss.PutResponse put(oss.PutRequest request);
    oss.GetResponse get(oss.GetRequest reuqest);
    oss.DeleteResponse delete(oss.DeleteRequest request);
}
