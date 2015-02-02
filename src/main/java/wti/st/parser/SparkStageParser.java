package wti.st.parser;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import wti.st.info.StageRecord;
import wti.st.info.TaskReadMethod;
import wti.st.info.TaskRecord;
import wti.st.info.TaskType;

import java.io.*;
import java.net.URI;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by guozhongxin on 15/1/30.
 */
public class SparkStageParser {

	private final String sparkLogDir;

	public SparkStageParser(String sparkLogDir) throws IOException {

//		Configuration conf = new Configuration();
//		URI uri = URI.create(sparkLogDir);
//		FileSystem fs = FileSystem.get(uri, conf);
//
//		if (!(fs.exists(new Path(sparkLogDir)))) {
//			// XXX
//			System.out.println("Path of the spark log directory is wrong");
//		}
		if (!sparkLogDir.endsWith(File.separator)) {
			sparkLogDir += File.separator;
		}
		this.sparkLogDir = sparkLogDir;
	}

	public List<StageRecord> stageParser(String appID) throws IOException {


		String appLogDir = sparkLogDir + appID + File.separator;

		String appLogFile = appLogDir + SparkLogLabel.EVENT_LOG_1_FILE;

		Configuration conf = new Configuration();
		URI uri = URI.create(appLogDir.toString());
		FileSystem fs = FileSystem.get(uri, conf);

		FSDataInputStream in = fs.open(new Path(appLogFile));
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String line = null;
		Gson gson = new Gson();
		List<StageRecord> stages = new ArrayList<StageRecord>();
		List<TaskRecord> tasks = new ArrayList<TaskRecord>();


		while ((line = br.readLine()) != null) {
			Map<String, Object> lineMap = gson.fromJson(line,
					new TypeToken<Map<String, Object>>() {
					}.getType());

			String eventName = (String) lineMap.get(SparkLogLabel.EVENT);

			if (eventName.equals(SparkLogLabel.STAGE_COMPLETE_EVENT)) {
				StageRecord stage = parserStage(lineMap, appID);
				stages.add(stage);
			}


			if (eventName.equals(SparkLogLabel.TASK_END_EVENT)) {
				TaskRecord task = parserTask(lineMap, appID);
				tasks.add(task);
			}
		}
		br.close();
		in.close();

		return computeMetics(stages, tasks);
	}

	public List<StageRecord> stageParserLocal(String filePath,String appID) throws IOException{
		List<StageRecord> stages = new ArrayList<StageRecord>();

		List<TaskRecord> tasks = new ArrayList<TaskRecord>();

		BufferedReader br = new BufferedReader(new FileReader(filePath));
		String line = null;
		Gson gson = new Gson();

		while ((line = br.readLine()) != null) {
			Map<String, Object> lineMap = gson.fromJson(line,
					new TypeToken<Map<String, Object>>() {
					}.getType());

			String eventName = (String) lineMap.get(SparkLogLabel.EVENT);

			if (eventName.equals(SparkLogLabel.STAGE_COMPLETE_EVENT)) {

				StageRecord stage = parserStage(lineMap, appID);
				stages.add(stage);
			}


			if (eventName.equals(SparkLogLabel.TASK_END_EVENT)) {
				TaskRecord task = parserTask(lineMap, appID);
				tasks.add(task);
			}
		}
		br.close();

		return computeMetics(stages, tasks);
	}

	private TaskRecord parserTask(Map<String, Object> lineMap, String appID) {
		Gson gson = new Gson();

		Object stageIDObj = lineMap.get(SparkLogLabel.STAGE_ID);
		Object stageAttIDObj = lineMap
				.get(SparkLogLabel.STAGE_ATTEMT_ID);

		int stageID = parserInt(stageIDObj);
		int stageAttemptID = parserInt(stageAttIDObj);

		StageRecord stage = new StageRecord(stageID,
				stageAttemptID, appID);

		Object taskTypeObj = lineMap.get(SparkLogLabel.TASK_TYPE);
		TaskType taskType = parserTaskType(taskTypeObj);
		String taskEndReason = lineMap.get(SparkLogLabel.TASK_END_REASON).toString();

		//"Task Info":{"Task ID":61,"Index":39,"Attempt":0,"Launch Time":1415256422721,"Executor ID":"8","Host":"bigant-test-003","Locality":"NODE_LOCAL","Speculative":false,"Getting Result Time":0,"Finish Time":1415256425620,"Failed":false,"Accumulables":[]}
		Object taskInfo = lineMap.get(SparkLogLabel.TASK_INFO);
		String taskInfoString = gson.toJson(taskInfo);

		Map<String, Object> taskInfoMap = gson.fromJson(taskInfoString,
				new TypeToken<Map<String, Object>>() {
				}.getType());

		// String index = taskInfoMap.get(SparkLogLabel.TASK_INDEX);

		int taskID = parserInt(taskInfoMap.get(SparkLogLabel.TASK_ID));
		int taskAttID = parserInt(taskInfoMap
				.get(SparkLogLabel.TASK_ATTEMT_ID));
		Timestamp startTime = parserTimestamp(
				taskInfoMap.get(SparkLogLabel.TASK_START_TIME));
		Timestamp endTime = parserTimestamp(
				taskInfoMap.get(SparkLogLabel.TASK_END_TIME));
		String nodeName = taskInfoMap.get(SparkLogLabel.TASK_NODE)
				.toString();
		String executorID = taskInfoMap.get(SparkLogLabel.TASK_EXECUTOR_ID).toString();
		String locality = taskInfoMap.get(SparkLogLabel.TASK_LOCALITY).toString();

		//"Task Metrics":{"Host Name":"bigant-test-003","Executor Deserialize Time":510,"Executor Run Time":2066,"Result Size":2309,"JVM GC Time":55,"Result Serialization Time":0,"Memory Bytes Spilled":0,"Disk Bytes Spilled":0,"Input Metrics":{"Data Read Method":"Hadoop","Bytes Read":134217728}}
		Object taskMetrics = lineMap.get(SparkLogLabel.TASK_METRICS);
		String taskMetricsString = gson.toJson(taskMetrics);

		Map<String, Object> taskMetricsMap = gson.fromJson(taskMetricsString,
				new TypeToken<Map<String, Object>>() {
				}.getType());

		long deserialTime = parserLong(taskMetricsMap.get(SparkLogLabel.TASK_DESERIALIZE_TIME));
		long serialTime = parserLong(taskMetricsMap.get(SparkLogLabel.TASK_SERIALIZE_TIME));
		long runTime = parserLong(taskMetricsMap.get(SparkLogLabel.TASK_RUN_TIME));
		long gcTime = parserLong(taskMetricsMap.get(SparkLogLabel.TASK_GC_TIME));

		long resultSize = parserLong(taskMetricsMap.get(SparkLogLabel.TASK_RESULT_SIZE));
		long memSpilled = parserLong(taskMetricsMap.get(SparkLogLabel.TASK_MEM_SPILLED));
		long diskSpilled = parserLong(taskMetricsMap.get(SparkLogLabel.TASK_DISK_SPILLED));

		TaskReadMethod readMethod = TaskReadMethod.Others;
		long bytesRead = 0l;

		long shuffleWriteTime = 0l;
		long shuffleWriteBytes = 0l;

		long shuffleReadRemoteBlocks = 0l;
		long shuffleReadLocalBlocks = 0l;
		long shuffleReadRemoteBytes = 0l;
		long shuffleFetchWaitTime = 0l;

		//"Input Metrics":{"Data Read Method":"Hadoop","Bytes Read":134217728}
		if (taskMetricsMap.containsKey(SparkLogLabel.TASK_INPUT_METRICS)) {
			Object inputMetrics = taskMetricsMap.get(SparkLogLabel.TASK_INPUT_METRICS);
			String inputMetricsString = gson.toJson(inputMetrics);
			Map<String, Object> inputMetricsMap = gson.fromJson(inputMetricsString,
					new TypeToken<Map<String, Object>>() {
					}.getType());

			readMethod = parserDataReadMethod(inputMetricsMap.get(SparkLogLabel.TASK_INPUT_METHOD));
			bytesRead = parserLong(inputMetricsMap.get(SparkLogLabel.TASK_BYTES_READ));
		}

		// "Shuffle Write Metrics":{"Shuffle Bytes Written":66757322,"Shuffle Write Time":138373180}
		if (taskMetricsMap.containsKey(SparkLogLabel.TASK_SHUFFLE_WRITE_METRICS)) {
			Object shuffleWriteMetrics = taskMetricsMap.get(SparkLogLabel.TASK_SHUFFLE_WRITE_METRICS);
			String shuffleWriteMetricsString = gson.toJson(shuffleWriteMetrics);
			Map<String, Object> shuffleWriteMetricsMap = gson.fromJson(shuffleWriteMetricsString,
					new TypeToken<Map<String, Object>>() {
					}.getType());

			shuffleWriteBytes = parserLong(shuffleWriteMetricsMap.get(SparkLogLabel.TASK_SHUFFLE_WRITE_BYTES));
			shuffleWriteTime = parserLong(shuffleWriteMetricsMap.get(SparkLogLabel.TASK_FETCH_WAIT_TIME));
		}

		// "Shuffle Read Metrics":{"Shuffle Finish Time":-1,"Remote Blocks Fetched":69,"Local Blocks Fetched":6,"Fetch Wait Time":14820,"Remote Bytes Read":62406189}
		if (taskMetricsMap.containsKey(SparkLogLabel.TASK_SHUFFLE_READ_METRICS)) {
			Object shuffleReadMetrics = taskMetricsMap.get(SparkLogLabel.TASK_SHUFFLE_READ_METRICS);
			String shuffleReadMetricsString = gson.toJson(shuffleReadMetrics);
			Map<String, Object> shuffleReadMetricsMap = gson.fromJson(shuffleReadMetricsString,
					new TypeToken<Map<String, Object>>() {
					}.getType());

			shuffleReadRemoteBlocks = parserLong(shuffleReadMetricsMap.get(SparkLogLabel.TASK_SHUFFLE_REMOTE_BLOCKS));
			shuffleReadLocalBlocks = parserLong(shuffleReadMetricsMap.get(SparkLogLabel.TASK_SHUFFLE_LOCAL_BLOCKS));
			shuffleReadRemoteBytes = parserLong(shuffleReadMetricsMap.get(SparkLogLabel.TASK_SHUFFLE_REMOTE_BYTES));
			shuffleFetchWaitTime = parserLong(shuffleReadMetricsMap.get(SparkLogLabel.TASK_FETCH_WAIT_TIME));

		}

		TaskRecord task = new TaskRecord(taskID, taskAttID, stage, taskType, startTime, endTime, nodeName, runTime, gcTime,
				deserialTime, serialTime, memSpilled, diskSpilled, bytesRead, readMethod, resultSize, shuffleWriteBytes,
				shuffleWriteTime, shuffleReadRemoteBlocks, shuffleReadLocalBlocks, shuffleReadRemoteBytes, shuffleFetchWaitTime);

		return task;
	}


	private StageRecord parserStage(Map<String, Object> lineMap, String appID) {

		Gson gson = new Gson();
		//"Task Info":{"Task ID":61,"Index":39,"Attempt":0,"Launch Time":1415256422721,"Executor ID":"8","Host":"bigant-test-003","Locality":"NODE_LOCAL","Speculative":false,"Getting Result Time":0,"Finish Time":1415256425620,"Failed":false,"Accumulables":[]}
		Object stageInfo = lineMap.get(SparkLogLabel.STAGE_INFO);
		String stageInfoString = gson.toJson(stageInfo);

		Map<String, Object> stageInfoMap = gson.fromJson(stageInfoString,
				new TypeToken<Map<String, Object>>() {
				}.getType());

		int stageID = parserInt(stageInfoMap.get(SparkLogLabel.STAGE_ID));
		int stageAttID = parserInt(stageInfoMap.get(SparkLogLabel.STAGE_ATTEMT_ID));
		String stageName = stageInfoMap.get(SparkLogLabel.STAGE_NAME).toString();
		int numTasks = parserInt(stageInfoMap.get(SparkLogLabel.STAGE_TASK_NUM));
		String stageDetail = stageInfoMap.get((SparkLogLabel.STAGE_DETAIL)).toString();
		Timestamp startTime = parserTimestamp(stageInfoMap.get(SparkLogLabel.STAGE_START_TIME));
		Timestamp endTime = parserTimestamp(stageInfoMap.get(SparkLogLabel.STAGE_END_TIME));

		StageRecord stageRecord = new StageRecord(stageID,stageAttID,appID,stageName,stageDetail,numTasks,null,
				startTime,endTime);

		return stageRecord;
	}

	private List<StageRecord> computeMetics(List<StageRecord> stages, List<TaskRecord> tasks) {

		for (TaskRecord taskRecord : tasks) {
			StageRecord stageRecord = taskRecord.getStage();

			int index = stages.indexOf(stageRecord);
			StageRecord stageDetail = stages.get(index);

			stageDetail.setStageType(taskRecord.getTaskType());
			stageDetail.setInputFromMem(stageDetail.getInputFromMem() + taskRecord.getMemRead());
			stageDetail.setInputFromHadoop(stageDetail.getInputFromHadoop() + taskRecord.getHadoopRead());
			stageDetail.setShuffleReadBytes(stageDetail.getShuffleReadBytes() + taskRecord.getShuffleReadRemoteBytes());
			stageDetail.setShuffleWriteBytes(stageDetail.getShuffleWriteBytes() + taskRecord.getShuffleWriteBytes());
			stageDetail.setShuffleFetchWaitTime(stageDetail.getShuffleFetchWaitTime() + taskRecord.getShuffleFetchWaitTime());
			stageDetail.setTasksGCTime(stageDetail.getTasksGCTime() + taskRecord.getGcTime());
			stageDetail.setTasksRunTime(stageDetail.getTasksRunTime() + taskRecord.getRunTime());

			stages.set(index, stageDetail);
		}
		return stages;
	}

	private int parserInt(Object obj) {

		if (obj instanceof Double) {
			return (int) Math.round((Double) obj);
		} else if (obj instanceof String) {
			Object object = Double.parseDouble((String) obj);
			return parserInt(object);
		} else {
			return -1;
		}
	}

	private long parserLong(Object obj) {
		if (obj instanceof Double) {
			return Math.round((Double) obj);
		} else if (obj instanceof String) {
			Object object = Double.parseDouble((String) obj);
			return parserLong(object);
		} else {
			return -1l;
		}
	}

	private TaskType parserTaskType(Object obj) {
		if (obj instanceof String) {
			String str = (String) obj;
			if (str.equals(TaskType.ResultTask.toString())) {
				return TaskType.ResultTask;
			} else if (str.equals(TaskType.ShuffleMapTask.toString())) {
				return TaskType.ShuffleMapTask;
			} else {
				return null;
			}
		}
		return null;
	}

	private TaskReadMethod parserDataReadMethod(Object obj) {
		if (obj instanceof String) {
			String str = obj.toString();
			if (str.equals(TaskReadMethod.Disk.toString())) {
				return TaskReadMethod.Disk;
			} else if (str.equals(TaskReadMethod.Hadoop.toString())) {
				return TaskReadMethod.Hadoop;
			} else if (str.equals(TaskReadMethod.Memory.toString())) {
				return TaskReadMethod.Memory;
			} else {
				return TaskReadMethod.Others;
			}
		}
		return null;
	}

	private Timestamp parserTimestamp(Object object) {
		if (object instanceof Double) {
			return new Timestamp(Math.round((Double) object));
		} else {
			return new Timestamp(0l);
		}
	}

	public static void main(String[] args) throws IOException {
		String dir = args[0]; //"hdfs://bigant-test-001:8020/spark110log"
		SparkStageParser stageParser = new SparkStageParser(
				dir);
		String appID = args[1];  //"wc-1412857997167"
		String output = args[2];

		List<StageRecord> stages = stageParser.stageParserLocal(dir,appID);

		FileWriter fw = new FileWriter(output,true);

		fw.write("stageID,stageAttID,appID,stageName,taskNum,submitTime,endTime,stageType,inputFromMem," +
				"inputFromHadoop,shuffleRead,shuffleWrite,shuffleFetchWaitTime,shuffleWriteTime,tasksRunTime," +
				"tasksGCTime\n");
		for (StageRecord stageRecord : stages) {
			String line = stageRecord.getStageID() + "," +
					stageRecord.getStageAttemptID() + "," +
					stageRecord.getAppID() + "," +
					stageRecord.getStageName() + "," +
					stageRecord.getTaskNum() + "," +
					stageRecord.getSubmitTime() + "," +
					stageRecord.getEndTime() + "," +
					stageRecord.getStageType() + "," +
					stageRecord.getInputFromMem() + "," +
					stageRecord.getInputFromHadoop() + "," +
					stageRecord.getShuffleReadBytes() + "," +
					stageRecord.getShuffleWriteBytes() + "," +
					stageRecord.getShuffleFetchWaitTime() + "," +
					stageRecord.getShuffleWriteTime() + "," +
					stageRecord.getTasksRunTime() +"," +
					stageRecord.getTasksGCTime() +"\n";
			fw.write(line);
		}
		fw.close();

	}
}
