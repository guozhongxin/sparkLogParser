package wti.st.info;

import java.sql.Timestamp;

public class SparkJobRecord
{
    private final String jobName;
    private final String jobID;
    private final Timestamp startTime;
    private final Timestamp endTime;
    
    public SparkJobRecord(String jobName, String jobID, Timestamp startTime, Timestamp endTime) {
        this.jobName = jobName;
        this.jobID = jobID;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public String getJobName()
    {
        return jobName;
    }

    public String getJobID()
    {
        return jobID;
    }

    public Timestamp getStartTime()
    {
        return startTime;
    }

    public Timestamp getEndTime()
    {
        return endTime;
    }

    @Override
    public String toString()
    {
	return "SparkJobRecord{" +
                "jobName='" + jobName + '\'' +
                ", jobID='" + jobID + '\'' +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                '}';    }
    
    
}
