package com._4paradigm.openmldb.memoryusagecompare;

import com.google.gson.annotations.Expose;

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
}
