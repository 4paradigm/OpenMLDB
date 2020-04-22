package com._4paradigm.rtidb.client;

import com._4paradigm.rtidb.client.schema.Table;
import com._4paradigm.rtidb.tablet.Tablet;

import java.util.ArrayList;
import java.util.List;

public class GetOption {
    private String tsName;
    private String idxName;
    private List<String> projection = new ArrayList<>();
    private long et  = 0;
    private Tablet.GetType stType = null;
    private Tablet.GetType etType = null;

    public long getEt() {
        return et;
    }

    public void setEt(long et) {
        this.et = et;
    }

    public Tablet.GetType getStType() {
        return stType;
    }

    public void setStType(Tablet.GetType stType) {
        this.stType = stType;
    }


    public Tablet.GetType getEtType() {
        return etType;
    }

    public void setEtType(Tablet.GetType etType) {
        this.etType = etType;
    }

    public String getIdxName() {
        return idxName;
    }

    public void setIdxName(String idxName) {
        this.idxName = idxName;
    }

    public List<String> getProjection() {
        return projection;
    }

    public void setProjection(List<String> projection) {
        this.projection = projection;
    }

    public String getTsName() {
        return tsName;
    }

    public void setTsName(String tsName) {
        this.tsName = tsName;
    }
}
