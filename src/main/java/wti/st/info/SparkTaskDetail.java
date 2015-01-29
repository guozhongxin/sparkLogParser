package wti.st.info;

import java.sql.Timestamp;

public class SparkTaskDetail
{
    private final int              taskID;
    private final int              taskAttemptID;
    private final SparkStageRecord stage;

    private Timestamp              startTime;
    private Timestamp              endTime;
    
    private String                 node;
    
    private String                 properties;
    private String		   timeTable;

    public SparkTaskDetail(int taskID, int taskAttemptID, SparkStageRecord stage)
    {
	this.taskID = taskID;
	this.taskAttemptID = taskAttemptID;
	this.stage = stage;
    }
    
    

    public SparkTaskDetail(int taskID, int taskAttemptID,
            SparkStageRecord stage, Timestamp startTime, Timestamp endTime,
            String node, String properties, String timeTable)
    {
	this.taskID = taskID;
	this.taskAttemptID = taskAttemptID;
	this.stage = stage;
	this.startTime = startTime;
	this.endTime = endTime;
	this.node = node;
	this.properties = properties;
	this.timeTable = timeTable;
    }

    @Override
    public boolean equals(Object obj)
    {
	if (this == obj) { return true; }
	if (obj instanceof SparkTaskDetail)
	{
	    SparkTaskDetail task = (SparkTaskDetail) obj;
	    if (this.taskID == task.taskID
		    && this.taskAttemptID == task.taskAttemptID
		    && this.stage.equals(task.stage))
	    {
		return true;
	    }
	    else
	    {
		return false;
	    }
	}
	else
	{
	    return false;
	}
    }

    public int getTaskID()
    {
	return taskID;
    }

    public int getTaskAttemptID()
    {
	return taskAttemptID;
    }

    public SparkStageRecord getStage()
    {
	return stage;
    }

    public Timestamp getStartTime()
    {
	return startTime;
    }

    public void setStartTime(Timestamp startTime)
    {
	this.startTime = startTime;
    }

    public Timestamp getEndTime()
    {
	return endTime;
    }

    public void setEndTime(Timestamp endTime)
    {
	this.endTime = endTime;
    }

    public String getProperties()
    {
	return properties;
    }

    public void setProperties(String properties)
    {
	this.properties = properties;
    }

    public String getNode()
    {
	return node;
    }

    public void setNode(String node)
    {
	this.node = node;
    }

    @Override
    public String toString()
    {
	return "SparkTaskDetail [taskID=" + taskID + ", taskAttemptID="
	        + taskAttemptID + ", stage=" + stage + ", startTime="
	        + startTime + ", endTime=" + endTime + ", node=" + node
	        + ", properties=" + properties + ", timeTable=" + timeTable
	        + "]";
    }

    
}
