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

	private String properties;
	private String timeTable;

	private long runTime;
	private long gcTime;
	private long deserialTime;  	//Executor Deserialize Time
	private long serializeTime;		//Result Serialization Time

	private long bytesRead;
	private TaskReadMethod readMethod;
	private long resultSize;

	private long memSpilled;		//Memory Bytes Spilled
	private long diskSpilled;		//Disk Bytes Spilled


	public TaskRecord(int taskID, int taskAttemptID, StageRecord stage) {
		this.taskID = taskID;
		this.taskAttemptID = taskAttemptID;
		this.stage = stage;
	}


	public TaskRecord(int taskID, int taskAttemptID,
					  StageRecord stage, Timestamp launchTime, Timestamp finishTime,
					  String node, String properties, String timeTable) {
		this.taskID = taskID;
		this.taskAttemptID = taskAttemptID;
		this.stage = stage;
		this.launchTime = launchTime;
		this.finishTime = finishTime;
		this.node = node;
		this.properties = properties;
		this.timeTable = timeTable;
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

	public String getProperties() {
		return properties;
	}

	public void setProperties(String properties) {
		this.properties = properties;
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
				", properties='" + properties + '\'' +
				", timeTable='" + timeTable + '\'' +
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
