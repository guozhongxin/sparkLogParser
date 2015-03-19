package wti.st.info;

import java.sql.Timestamp;
import java.util.List;

public class StageRecord {
	private final int stageID;
	private final int stageAttemptID;

	private final String appID;

	private String stageName;
	private String stageDitial;

	private int taskNum;
	private List<RDDInfo> rdds;

	private Timestamp submitTime;
	private Timestamp endTime;

	//need to compute
	private TaskType stageType;

	private long inputFromMem;
	private long inputFromHadoop;
	private long inputFromDisk;
	private long shuffleReadBytes;
	private long shuffleWriteBytes;

	private long shuffleFetchWaitTime;
	private long shuffleWriteTime;

	private long shuffleReadLocalBlocks;
	private long shuffleReadRemoteBlocks;

	private long tasksRunTime;
	private long tasksGCTime;
	private long tasksLastTime;
	private long tasksDeserialTime;
	private long tasksSerializeTime;

	private long memSpilled;
	private long diskSpilled;

	private long resultSize;

	public StageRecord(int stageID, int stageAttemprID, String appID) {
		this.stageID = stageID;
		this.stageAttemptID = stageAttemprID;
		this.appID = appID;
	}

	public StageRecord(int stageID, int stageAttemptID, String appID, String stageName, String stageDitial, int taskNum, List<RDDInfo> rdds, Timestamp submitTime, Timestamp endTime) {
		this.stageID = stageID;
		this.stageAttemptID = stageAttemptID;
		this.appID = appID;
		this.stageName = stageName;
		this.stageDitial = stageDitial;
		this.taskNum = taskNum;
		this.rdds = rdds;
		this.submitTime = submitTime;
		this.endTime = endTime;
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
					&& this.appID.equals(sparkStage.appID)) {
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

	public String getAppID() {
		return appID;
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

	public TaskType getStageType() {
		return stageType;
	}

	public void setStageType(TaskType stageType) {
		this.stageType = stageType;
	}

	public long getInputFromMem() {
		return inputFromMem;
	}

	public void setInputFromMem(long inputFromMem) {
		this.inputFromMem = inputFromMem;
	}

	public long getInputFromHadoop() {
		return inputFromHadoop;
	}

	public void setInputFromHadoop(long inputFromHadoop) {
		this.inputFromHadoop = inputFromHadoop;
	}

	public long getInputFromDisk() {
		return inputFromDisk;
	}

	public void setInputFromDisk(long inputFromDisk) {
		this.inputFromDisk = inputFromDisk;
	}

	public long getShuffleReadBytes() {
		return shuffleReadBytes;
	}

	public void setShuffleReadBytes(long shuffleReadBytes) {
		this.shuffleReadBytes = shuffleReadBytes;
	}

	public long getShuffleWriteBytes() {
		return shuffleWriteBytes;
	}

	public void setShuffleWriteBytes(long shuffleWriteBytes) {
		this.shuffleWriteBytes = shuffleWriteBytes;
	}

	public long getShuffleFetchWaitTime() {
		return shuffleFetchWaitTime;
	}

	public void setShuffleFetchWaitTime(long shuffleFetchWaitTime) {
		this.shuffleFetchWaitTime = shuffleFetchWaitTime;
	}

	public long getShuffleWriteTime() {
		return shuffleWriteTime;
	}

	public void setShuffleWriteTime(long shuffleWriteTime) {
		this.shuffleWriteTime = shuffleWriteTime;
	}

	public long getTasksRunTime() {
		return tasksRunTime;
	}

	public void setTasksRunTime(long tasksRunTime) {
		this.tasksRunTime = tasksRunTime;
	}

	public long getTasksGCTime() {
		return tasksGCTime;
	}

	public void setTasksGCTime(long tasksGCTime) {
		this.tasksGCTime = tasksGCTime;
	}

	public long getShuffleReadLocalBlocks() {
		return shuffleReadLocalBlocks;
	}

	public void setShuffleReadLocalBlocks(long shuffleReadLocalBlocks) {
		this.shuffleReadLocalBlocks = shuffleReadLocalBlocks;
	}

	public long getShuffleReadRemoteBlocks() {
		return shuffleReadRemoteBlocks;
	}

	public void setShuffleReadRemoteBlocks(long shuffleReadRemoteBlocks) {
		this.shuffleReadRemoteBlocks = shuffleReadRemoteBlocks;
	}

	public long getTasksLastTime() {
		return tasksLastTime;
	}

	public void setTasksLastTime(long tasksLastTime) {
		this.tasksLastTime = tasksLastTime;
	}

	public long getTasksDeserialTime() {
		return tasksDeserialTime;
	}

	public void setTasksDeserialTime(long tasksDeserialTime) {
		this.tasksDeserialTime = tasksDeserialTime;
	}

	public long getTasksSerializeTime() {
		return tasksSerializeTime;
	}

	public void setTasksSerializeTime(long tasksSerializeTime) {
		this.tasksSerializeTime = tasksSerializeTime;
	}

	public long getMemSpilled() {
		return memSpilled;
	}

	public void setMemSpilled(long memSpilled) {
		this.memSpilled = memSpilled;
	}

	public long getDiskSpilled() {
		return diskSpilled;
	}

	public void setDiskSpilled(long diskSpilled) {
		this.diskSpilled = diskSpilled;
	}

	public long getResultSize() {
		return resultSize;
	}

	public void setResultSize(long resultSize) {
		this.resultSize = resultSize;
	}

	@Override
	public String toString() {
		return "StageRecord{" +
				"stageID=" + stageID +
				", stageAttemptID=" + stageAttemptID +
				", appID='" + appID + '\'' +
				", stageName='" + stageName + '\'' +
				", stageDitial='" + stageDitial + '\'' +
				", taskNum=" + taskNum +
				", rdds=" + rdds +
				", submitTime=" + submitTime +
				", endTime=" + endTime +
				", stageType=" + stageType +
				", inputFromMem=" + inputFromMem +
				", inputFromHadoop=" + inputFromHadoop +
				", inputFromDisk=" + inputFromDisk +
				", shuffleReadBytes=" + shuffleReadBytes +
				", shuffleWriteBytes=" + shuffleWriteBytes +
				", shuffleFetchWaitTime=" + shuffleFetchWaitTime +
				", shuffleWriteTime=" + shuffleWriteTime +
				", shuffleReadLocalBlocks=" + shuffleReadLocalBlocks +
				", shuffleReadRemoteBlocks=" + shuffleReadRemoteBlocks +
				", tasksRunTime=" + tasksRunTime +
				", tasksGCTime=" + tasksGCTime +
				", tasksLastTime=" + tasksLastTime +
				", tasksDeserialTime=" + tasksDeserialTime +
				", tasksSerializeTime=" + tasksSerializeTime +
				", memSpilled=" + memSpilled +
				", diskSpilled=" + diskSpilled +
				", resultSize=" + resultSize +
				'}';
	}
}
