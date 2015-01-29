package wti.st.info;

public class SparkStageRecord
{
    private final int    stageID;
    private final int    stageAttemptID;

    private final String jobID;
    private String       stageName;

    public SparkStageRecord(int stageID, int stageAttemprID, String jobID,
	    String stageName)
    {
	this.stageID = stageID;
	this.stageAttemptID = stageAttemprID;
	this.jobID = jobID;
	this.stageName = stageName;
    }

    public SparkStageRecord(int stageID, int stageAttemprID, String jobID)
    {
	this.stageID = stageID;
	this.stageAttemptID = stageAttemprID;
	this.jobID = jobID;
    }

    @Override
    public boolean equals(Object obj)
    {
	if (this == obj) { return true; }
	if (obj instanceof SparkStageRecord)
	{
	    SparkStageRecord sparkStage = (SparkStageRecord) obj;
	    if (this.stageID == sparkStage.stageID
		    && this.stageAttemptID == sparkStage.stageAttemptID
		    && this.jobID.equals(sparkStage.jobID))
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

    public int getStageID()
    {
	return stageID;
    }

    public int getStageAttemptID()
    {
	return stageAttemptID;
    }

    public String getStageName()
    {
	return stageName;
    }

    @Override
    public String toString()
    {
	return "SparkStageRecord [stageID=" + stageID + ", stageAttemptID="
	        + stageAttemptID + ", jobID=" + jobID + ", stageName="
	        + stageName + "]";
    }

    
}
