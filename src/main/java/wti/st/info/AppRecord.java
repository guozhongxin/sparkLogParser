package wti.st.info;

import java.sql.Timestamp;

public class AppRecord {
    private final String appName;
	private final String appID;
    private final Timestamp startTime;
    private final Timestamp endTime;

    public AppRecord(String appName, String appID, Timestamp startTime, Timestamp endTime) {
        this.appName = appName;
		this.appID = appID;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public String getAppName() {
        return appName;
    }

    public Timestamp getStartTime() {
        return startTime;
    }

    public Timestamp getEndTime() {
        return endTime;
    }

	@Override
	public String toString() {
		return "AppRecord{" +
				"appName='" + appName + '\'' +
				", appID='" + appID + '\'' +
				", startTime=" + startTime +
				", endTime=" + endTime +
				'}';
	}

}
