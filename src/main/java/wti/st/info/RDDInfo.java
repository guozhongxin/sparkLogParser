package wti.st.info;

/**
 * Created by guozhongxin on 2015/1/30.
 */
public class RDDInfo {
	private final int rddID;

	private String rddName;

	private int patitionsNum;
	private int cachedPatNum;

	private long memSize;
	private long tachyonSize;
	private long diskSize;

	// XXX
	private String storageLevel;

	public RDDInfo(int rddID) {
		this.rddID = rddID;
	}

	@Override
	public String toString() {
		return "RDDInfo{" +
				"rddID=" + rddID +
				", rddName='" + rddName + '\'' +
				", patitionsNum=" + patitionsNum +
				", cachedPatNum=" + cachedPatNum +
				", memSize=" + memSize +
				", tachyonSize=" + tachyonSize +
				", diskSize=" + diskSize +
				", storageLevel='" + storageLevel + '\'' +
				'}';
	}

	public int getRddID() {
		return rddID;
	}

	public String getRddName() {
		return rddName;
	}

	public void setRddName(String rddName) {
		this.rddName = rddName;
	}

	public int getPatitionsNum() {
		return patitionsNum;
	}

	public void setPatitionsNum(int patitionsNum) {
		this.patitionsNum = patitionsNum;
	}

	public int getCachedPatNum() {
		return cachedPatNum;
	}

	public void setCachedPatNum(int cachedPatNum) {
		this.cachedPatNum = cachedPatNum;
	}

	public long getMemSize() {
		return memSize;
	}

	public void setMemSize(long memSize) {
		this.memSize = memSize;
	}

	public long getTachyonSize() {
		return tachyonSize;
	}

	public void setTachyonSize(long tachyonSize) {
		this.tachyonSize = tachyonSize;
	}

	public long getDiskSize() {
		return diskSize;
	}

	public void setDiskSize(long diskSize) {
		this.diskSize = diskSize;
	}

	public String getStorageLevel() {
		return storageLevel;
	}

	public void setStorageLevel(String storageLevel) {
		this.storageLevel = storageLevel;
	}
}
