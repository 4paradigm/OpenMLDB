package com._4paradigm.openmldb.memoryusagecompare;

import com.google.gson.annotations.Expose;
import org.apache.commons.csv.CSVRecord;

public class TalkingData {
    public String ip;
    @Expose
    public int app;
    @Expose
    public int device;
    @Expose
    public int os;
    @Expose
    public int channel;
    @Expose
    public String clickTime;
    @Expose
    public int isAttribute;

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getApp() {
        return app;
    }

    public void setApp(int app) {
        this.app = app;
    }

    public int getDevice() {
        return device;
    }

    public void setDevice(int device) {
        this.device = device;
    }

    public int getOs() {
        return os;
    }

    public void setOs(int os) {
        this.os = os;
    }

    public int getChannel() {
        return channel;
    }

    public void setChannel(int channel) {
        this.channel = channel;
    }

    public String getClickTime() {
        return clickTime;
    }

    public void setClickTime(String click_time) {
        this.clickTime = click_time;
    }

    public int getIsAttribute() {
        return isAttribute;
    }

    public void setIsAttribute(int isAttribute) {
        this.isAttribute = isAttribute;
    }

    @Override
    public String toString() {
        return "TalkingData{" +
                "app=" + app +
                ", ip='" + ip + '\'' +
                ", device=" + device +
                ", os=" + os +
                ", channel=" + channel +
                ", clickTime='" + clickTime + '\'' +
                ", isAttribute=" + isAttribute +
                '}';
    }

    // convert CSVRecord to TalkingData
    public static TalkingData from(CSVRecord record) {
        TalkingData td = new TalkingData();
        String ip = record.get("ip");
        td.setIp(ip);
        td.setApp(Integer.parseInt(record.get("app")));
        td.setDevice(Integer.parseInt(record.get("device")));
        td.setOs(Integer.parseInt(record.get("os")));
        td.setChannel(Integer.parseInt(record.get("channel")));
        td.setClickTime(record.get("click_time"));
        td.setIsAttribute(Integer.parseInt(record.get("is_attributed")));
        return td;
    }
}
