package com._4paradigm.pbrpc;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

public class FakeRpcController implements RpcController {

    public void reset() {
        // TODO Auto-generated method stub

    }

    public boolean failed() {
        // TODO Auto-generated method stub
        return false;
    }

    public String errorText() {
        // TODO Auto-generated method stub
        return null;
    }

    public void startCancel() {
        // TODO Auto-generated method stub

    }

    public void setFailed(String reason) {
        // TODO Auto-generated method stub

    }

    public boolean isCanceled() {
        // TODO Auto-generated method stub
        return false;
    }

    public void notifyOnCancel(RpcCallback<Object> callback) {
        // TODO Auto-generated method stub

    }

}
