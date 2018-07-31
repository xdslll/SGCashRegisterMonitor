package com.sg.cash.hadoop.sqlserver;

/**
 * @author xiads
 * @date 2018/5/19
 * @since
 */
public class HupuData {

    public int groupId;

    public String groupName;

    public int machineId;

    public String machineName;

    public String dateStr;

    public long timestamp;

    public String ip;

    public int openTime;

    public int activeTime;

    public String machineNo;

    public int getGroupId() {
        return groupId;
    }

    public void setGroupId(int groupId) {
        this.groupId = groupId;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public int getMachineId() {
        return machineId;
    }

    public void setMachineId(int machineId) {
        this.machineId = machineId;
    }

    public String getMachineName() {
        return machineName;
    }

    public void setMachineName(String machineName) {
        this.machineName = machineName;
    }

    public String getDateStr() {
        return dateStr;
    }

    public void setDateStr(String dateStr) {
        this.dateStr = dateStr;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getOpenTime() {
        return openTime;
    }

    public void setOpenTime(int openTime) {
        this.openTime = openTime;
    }

    public int getActiveTime() {
        return activeTime;
    }

    public void setActiveTime(int activeTime) {
        this.activeTime = activeTime;
    }

    public String getMachineNo() {
        return machineNo;
    }

    public void setMachineNo(String machineNo) {
        this.machineNo = machineNo;
    }

    @Override
    public String toString() {
        return "HupuData{" +
                "groupId=" + groupId +
                ", groupName='" + groupName + '\'' +
                ", machineId=" + machineId +
                ", machineName='" + machineName + '\'' +
                ", dateStr='" + dateStr + '\'' +
                ", timestamp=" + timestamp +
                ", ip='" + ip + '\'' +
                ", openTime=" + openTime +
                ", activeTime=" + activeTime +
                '}';
    }
}
