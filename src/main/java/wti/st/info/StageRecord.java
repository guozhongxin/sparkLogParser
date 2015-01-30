package wti.st.info;

import java.sql.Timestamp;
import java.util.List;

public class StageRecord {
	private final int stageID;
	private final int stageAttemptID;

	private final String jobID;

	private String stageName;
	private String stageDitial;

	private int taskNum;
	private List<RDDInfo> rdds;

	private Timestamp submitTime;
	private Timestamp endTime;

	//need to compute
	private StageType stageType;
	private long shuffleReadFromHDFS;
	private long shuffleReadFromMem;
	private long shuffleWrite;

	public StageRecord(int stageID, int stageAttemprID, String jobID,
					   String stageName) {
		this.stageID = stageID;
		this.stageAttemptID = stageAttemprID;
		this.jobID = jobID;
		this.stageName = stageName;
	}

	public StageRecord(int stageID, int stageAttemprID, String jobID) {
		this.stageID = stageID;
		this.stageAttemptID = stageAttemprID;
		this.jobID = jobID;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj instanceof StageRecord) {
			StageRecord sparkStage = (StageRecord) obj;
			if (this.stageID == sparkStage.stageID
					&& this.stageAttemptID == sparkStage.stageAttemptID
					&& this.jobID.equals(sparkStage.jobID)) {
				return true;
			} else {
				return false;
			}
		} else {
			return false;
		}

	}

	public int getStageID() {
		return stageID;
	}

	public int getStageAttemptID() {
		return stageAttemptID;
	}

	public String getStageName() {
		return stageName;
	}

	public String getJobID() {
		return jobID;
	}

	public void setStageName(String stageName) {
		this.stageName = stageName;
	}

	public String getStageDitial() {
		return stageDitial;
	}

	public void setStageDitial(String stageDitial) {
		this.stageDitial = stageDitial;
	}

	public int getTaskNum() {
		return taskNum;
	}

	public void setTaskNum(int taskNum) {
		this.taskNum = taskNum;
	}

	public List<RDDInfo> getRdds() {
		return rdds;
	}

	public void setRdds(List<RDDInfo> rdds) {
		this.rdds = rdds;
	}

	public Timestamp getSubmitTime() {
		return submitTime;
	}

	public void setSubmitTime(Timestamp submitTime) {
		this.submitTime = submitTime;
	}

	public Timestamp getEndTime() {
		return endTime;
	}

	public void setEndTime(Timestamp endTime) {
		this.endTime = endTime;
	}

	public StageType getStageType() {
		return stageType;
	}

	public void setStageType(StageType stageType) {
		this.stageType = stageType;
	}

	public long getShuffleReadFromHDFS() {
		return shuffleReadFromHDFS;
	}

	public void setShuffleReadFromHDFS(long shuffleReadFromHDFS) {
		this.shuffleReadFromHDFS = shuffleReadFromHDFS;
	}

	public long getShuffleReadFromMem() {
		return shuffleReadFromMem;
	}

	public void setShuffleReadFromMem(long shuffleReadFromMem) {
		this.shuffleReadFromMem = shuffleReadFromMem;
	}

	public long getShuffleWrite() {
		return shuffleWrite;
	}

	public void setShuffleWrite(long shuffleWrite) {
		this.shuffleWrite = shuffleWrite;
	}

	@Override
	public String toString() {
		return "StageRecord{" +
				"stageID=" + stageID +
				", stageAttemptID=" + stageAttemptID +
				", jobID='" + jobID + '\'' +
				", stageName='" + stageName + '\'' +
				", stageDitial='" + stageDitial + '\'' +
				", taskNum=" + taskNum +
				", rdds=" + rdds +
				", submitTime=" + submitTime +
				", endTime=" + endTime +
				", stageType=" + stageType +
				", shuffleReadFromHDFS=" + shuffleReadFromHDFS +
				", shuffleReadFromMem=" + shuffleReadFromMem +
				", shuffleWrite=" + shuffleWrite +
				'}';
	}


}
