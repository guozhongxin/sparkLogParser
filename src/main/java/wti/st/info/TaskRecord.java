package wti.st.info;

import java.sql.Timestamp;

public class TaskRecord {
	private final int taskID;
	private final int taskAttemptID;
	private final StageRecord stage;

	private TaskType taskType;

	private Timestamp launchTime;
	private Timestamp finishTime;

	private String node;

	private long runTime;
	private long gcTime;
	private long deserialTime;  	//Executor Deserialize Time
	private long serializeTime;		//Result Serialization Time

	private long resultSize;

	private long memSpilled;		//Memory Bytes Spilled
	private long diskSpilled;		//Disk Bytes Spilled

	// may do not appear
	private long bytesRead;
	private TaskReadMethod readMethod;

	private long shuffleWriteBytes;
	private long shuffleWriteTime;

	private long shuffleReadRemoteBlocks;
	private long shuffleReadLocalBlocks;
	private long shuffleReadRemoteBytes;
	private long shuffleFetchWaitTime;


	public TaskRecord(int taskID, int taskAttemptID, StageRecord stage) {
		this.taskID = taskID;
		this.taskAttemptID = taskAttemptID;
		this.stage = stage;
	}

	public TaskRecord(int taskID, int taskAttemptID, StageRecord stage, TaskType taskType, Timestamp launchTime, Timestamp finishTime, String node, long runTime, long gcTime, long deserialTime, long serializeTime, long memSpilled, long diskSpilled, long bytesRead, TaskReadMethod readMethod, long resultSize, long shuffleWriteBytes, long shuffleWriteTime, long shuffleReadRemoteBlocks, long shuffleReadLocalBlocks, long shuffleReadRemoteBytes, long shuffleFetchWaitTime) {
		this.taskID = taskID;
		this.taskAttemptID = taskAttemptID;
		this.stage = stage;
		this.taskType = taskType;
		this.launchTime = launchTime;
		this.finishTime = finishTime;
		this.node = node;
		this.runTime = runTime;
		this.gcTime = gcTime;
		this.deserialTime = deserialTime;
		this.serializeTime = serializeTime;
		this.memSpilled = memSpilled;
		this.diskSpilled = diskSpilled;
		this.bytesRead = bytesRead;
		this.readMethod = readMethod;
		this.resultSize = resultSize;
		this.shuffleWriteBytes = shuffleWriteBytes;
		this.shuffleWriteTime = shuffleWriteTime;
		this.shuffleReadRemoteBlocks = shuffleReadRemoteBlocks;
		this.shuffleReadLocalBlocks = shuffleReadLocalBlocks;
		this.shuffleReadRemoteBytes = shuffleReadRemoteBytes;
		this.shuffleFetchWaitTime = shuffleFetchWaitTime;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj instanceof TaskRecord) {
			TaskRecord task = (TaskRecord) obj;
			if (this.taskID == task.taskID
					&& this.taskAttemptID == task.taskAttemptID
					&& this.stage.equals(task.stage)) {
				return true;
			} else {
				return false;
			}
		} else {
			return false;
		}
	}

	public int getTaskID() {
		return taskID;
	}

	public int getTaskAttemptID() {
		return taskAttemptID;
	}

	public StageRecord getStage() {
		return stage;
	}

	public Timestamp getLaunchTime() {
		return launchTime;
	}

	public void setLaunchTime(Timestamp launchTime) {
		this.launchTime = launchTime;
	}

	public Timestamp getFinishTime() {
		return finishTime;
	}

	public void setFinishTime(Timestamp finishTime) {
		this.finishTime = finishTime;
	}

	public String getNode() {
		return node;
	}

	public void setNode(String node) {
		this.node = node;
	}

	public TaskType getTaskType() {
		return taskType;
	}

	public void setTaskType(TaskType taskType) {
		this.taskType = taskType;
	}

	public long getRunTime() {
		return runTime;
	}

	public void setRunTime(long runTime) {
		this.runTime = runTime;
	}

	public long getGcTime() {
		return gcTime;
	}

	public void setGcTime(long gcTime) {
		this.gcTime = gcTime;
	}

	public long getDeserialTime() {
		return deserialTime;
	}

	public void setDeserialTime(long deserialTime) {
		this.deserialTime = deserialTime;
	}

	public long getSerializeTime() {
		return serializeTime;
	}

	public void setSerializeTime(long serializeTime) {
		this.serializeTime = serializeTime;
	}

	public long getBytesRead() {
		return bytesRead;
	}

	public void setBytesRead(long bytesRead) {
		this.bytesRead = bytesRead;
	}

	public TaskReadMethod getReadMethod() {
		return readMethod;
	}

	public void setReadMethod(TaskReadMethod readMethod) {
		this.readMethod = readMethod;
	}

	public long getResultSize() {
		return resultSize;
	}

	public void setResultSize(long resultSize) {
		this.resultSize = resultSize;
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

	//
	public long getMemRead(){
		if (this.readMethod.equals(TaskReadMethod.Memory)){
			return this.bytesRead;
		}else {
			return 0l;
		}
	}
	public long getHadoopRead(){
		if (this.readMethod.equals(TaskReadMethod.Hadoop)){
			return this.bytesRead;
		}else {
			return 0l;
		}

	}

	public long getDiskRead(){
		if (this.readMethod.equals(TaskReadMethod.Disk)){
			return this.bytesRead;
		}else {
			return 0l;
		}
	}


	public long getShuffleWriteBytes() {
		return shuffleWriteBytes;
	}

	public void setShuffleWriteBytes(long shuffleWriteBytes) {
		this.shuffleWriteBytes = shuffleWriteBytes;
	}

	public long getShuffleWriteTime() {
		return shuffleWriteTime;
	}

	public void setShuffleWriteTime(long shuffleWriteTime) {
		this.shuffleWriteTime = shuffleWriteTime;
	}

	public long getShuffleReadRemoteBlocks() {
		return shuffleReadRemoteBlocks;
	}

	public void setShuffleReadRemoteBlocks(long shuffleReadRemoteBlocks) {
		this.shuffleReadRemoteBlocks = shuffleReadRemoteBlocks;
	}

	public long getShuffleReadLocalBlocks() {
		return shuffleReadLocalBlocks;
	}

	public void setShuffleReadLocalBlocks(long shuffleReadLocalBlocks) {
		this.shuffleReadLocalBlocks = shuffleReadLocalBlocks;
	}

	public long getShuffleReadRemoteBytes() {
		return shuffleReadRemoteBytes;
	}

	public void setShuffleReadRemoteBytes(long shuffleReadRemoteBytes) {
		this.shuffleReadRemoteBytes = shuffleReadRemoteBytes;
	}

	public long getShuffleFetchWaitTime() {
		return shuffleFetchWaitTime;
	}

	public void setShuffleFetchWaitTime(long shuffleFetchWaitTime) {
		this.shuffleFetchWaitTime = shuffleFetchWaitTime;
	}

	@Override
	public String toString() {
		return "TaskDetail{" +
				"taskID=" + taskID +
				", taskAttemptID=" + taskAttemptID +
				", stage=" + stage +
				", taskType=" + taskType +
				", launchTime=" + launchTime +
				", finishTime=" + finishTime +
				", node='" + node + '\'' +
				", runTime=" + runTime +
				", gcTime=" + gcTime +
				", deserialTime=" + deserialTime +
				", serializeTime=" + serializeTime +
				", bytesRead=" + bytesRead +
				", readMethod=" + readMethod +
				", resultSize=" + resultSize +
				", memSpilled=" + memSpilled +
				", diskSpilled=" + diskSpilled +
				'}';
	}

}
