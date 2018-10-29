package com.sg.cash.model;

/**
 * @author xiads
 * @date 2018/9/25
 * @since
 */
public class MachineUsage {

    private String city;
    private String smallArea;
    private String storeNo;
    private String storeName;
    private Integer machineNum;
    private Double usageNum;
    private Double usagePercent;
    private Long createDt;

    public Long getCreateDt() {
        return createDt;
    }

    public void setCreateDt(Long createDt) {
        this.createDt = createDt;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getSmallArea() {
        return smallArea;
    }

    public void setSmallArea(String smallArea) {
        this.smallArea = smallArea;
    }

    public String getStoreNo() {
        return storeNo;
    }

    public void setStoreNo(String storeNo) {
        this.storeNo = storeNo;
    }

    public String getStoreName() {
        return storeName;
    }

    public void setStoreName(String storeName) {
        this.storeName = storeName;
    }

    public Integer getMachineNum() {
        return machineNum;
    }

    public void setMachineNum(Integer machineNum) {
        this.machineNum = machineNum;
    }

    public Double getUsageNum() {
        return usageNum;
    }

    public void setUsageNum(Double usageNum) {
        this.usageNum = usageNum;
    }

    public Double getUsagePercent() {
        return usagePercent;
    }

    public void setUsagePercent(Double usagePercent) {
        this.usagePercent = usagePercent;
    }

    @Override
    public String toString() {
        return "MachineUsage{" +
                "city='" + city + '\'' +
                ", smallArea='" + smallArea + '\'' +
                ", storeNo='" + storeNo + '\'' +
                ", storeName='" + storeName + '\'' +
                ", machineNum=" + machineNum +
                ", usageNum=" + usageNum +
                ", usagePercent=" + usagePercent +
                ", createDt=" + createDt +
                '}';
    }
}
