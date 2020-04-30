package rtidb.blobserver;

import com._4paradigm.rtidb.object_storage_server.ObjectStorage;


public interface BlobServer {
    ObjectStorage.PutResponse put(ObjectStorage.PutRequest request);
    ObjectStorage.GetResponse get(ObjectStorage.GetRequest reuqest);
    ObjectStorage.GeneralResponse delete(ObjectStorage.DeleteRequest request);
}
