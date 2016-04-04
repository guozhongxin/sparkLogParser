package wti.st.parser;

import wti.st.info.StageRecord;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Created by guozhongxin on 2015/3/1.
 */
public class LocalParser {
	public List<String> listApps(String dirPath) {
		List<String> appIDs = new ArrayList<String>();

		File logFilesDir = new File(dirPath);
		File[] logFiles = logFilesDir.listFiles();
		for (File logFile : logFiles) {
			String appID = logFile.getName();

			String appCompleteFileName = logFile.getPath().toString() + File.separator + SparkLogLabel.APPLICATION_COMPLETE_FILE;

			File appCompleteFile = new File(appCompleteFileName);
			if (logFile.isDirectory() && appCompleteFile.exists()) {
				appIDs.add(appID);
			}else if (appID.startsWith("app") && logFile.isFile() && !appID.endsWith(".inprogress")){
				appIDs.add(appID);
			}
		}
		return appIDs;
	}

	public void localParser(String dir ,String output, List<String> appIDs ) throws IOException {

		File outputFile = new File(output);
		HashSet<String> parseredAppsID = new HashSet<String>();

		if (outputFile.exists()) {
			BufferedReader br = new BufferedReader(new FileReader(output));
			String line = null;
			System.out.println(output);
			while((line = br.readLine()) != null){
				String appID = line.split(",")[2];
				System.out.println(appID);
				parseredAppsID.add(appID);
			}
			br.close();
		}
		//XXX
		System.out.println("#### parseredAppsID: ");
		for (String s : parseredAppsID) {
			System.out.println(s);
		}


		FileWriter fw = new FileWriter(output,true);

		fw.write("stageID,stageAttID,appID,appName,stageName,startTimeStampt,taskNum,runningTime,stageType," +
				"inputFromHadoop,inputFromMem,inputFromDisk," +
				"outputBytes,outputRecords," +
				"shuffleRead,shuffleWrite,shuffleReadLocalBlocks,shuffleReadRemoteBlocks," +
				"diskSpilled,memSpilled,resultSize," +
				"shuffleFetchWaitTime,shuffleWriteTime,tasksDeserialTime,tasksSerializeTime," +
				"tasksRunTime,tasksGCTime,tasksLastTime," +
				"bytesWrittenVar,recordsWrittenVar," +
				"shuffleReadVar,shuffleWriteVar,shuffleFetchWaitTimeVar,shuffleWriteTimeVar,shuffleReadRemoteBlocksVar," +
				"tasksDeserialTimeVar,tasksSerializeTimeVar,tasksRunTimeVar,tasksGCTimeVar,tasksLastTimeVar," +
                "readBytes,writeBytes,bytesIn,bytesOut," +
                "cpuPara,diskPara,netPara,cpuPara_1,diskPara_1,netPara_1,count\n");
		for (String appID : appIDs) {

			if (parseredAppsID.contains(appID)){
				continue;
			}

			SparkStageParser stageParser = new SparkStageParser(dir);

			String eventFile ="";
			File applog = new File(dir + File.separator + appID);
			if (applog.isDirectory()){
				eventFile = dir + File.separator + appID + File.separator + SparkLogLabel.EVENT_LOG_1_FILE;
			} else if (applog.isFile()){
				eventFile = dir + File.separator + appID;
			}

			System.out.println("## Parser appID:	" + appID);

			List<StageRecord> stages = stageParser.stageParserLocal(eventFile,appID);

			for (StageRecord stageRecord : stages) {
				long startTimeStamp = stageRecord.getSubmitTime().getTime();
				long endTimeStamp = stageRecord.getEndTime().getTime();
				long stageRunningTime = endTimeStamp - startTimeStamp;
				String line = stageRecord.getStageID() + "," +
						stageRecord.getStageAttemptID() + "," +
						stageRecord.getAppID() + "," +
						stageRecord.getAppName() + "," +
						stageRecord.getStageName() + "," +
						stageRecord.getSubmitTime()+","+
						stageRecord.getTaskNum() + "," +
						stageRunningTime + "," +
						stageRecord.getStageType() + "," +
						stageRecord.getInputFromHadoop() + "," +
						stageRecord.getInputFromMem() + "," +
						stageRecord.getInputFromDisk() + "," +
						stageRecord.getBytesWritten()  + "," +
						stageRecord.getRecordWritten() + "," +
						stageRecord.getShuffleReadBytes() + "," +
						stageRecord.getShuffleWriteBytes() + "," +
						stageRecord.getShuffleReadLocalBlocks() + "," +
						stageRecord.getShuffleReadRemoteBlocks() + "," +
						stageRecord.getDiskSpilled() + "," +
						stageRecord.getMemSpilled() + "," +
						stageRecord.getResultSize() + "," +
						stageRecord.getShuffleFetchWaitTime() + "," +
						stageRecord.getShuffleWriteTime() + "," +
						stageRecord.getTasksDeserialTime() + "," +
						stageRecord.getTasksSerializeTime() + "," +
						stageRecord.getTasksRunTime() +"," +
						stageRecord.getTasksGCTime() + "," +
						stageRecord.getTasksLastTime() +"," +
						//add statistic
						stageRecord.getBytesWrittenVar()  + "," +
						stageRecord.getRecordWrittenVar()  + "," +
						stageRecord.getShuffleReadBytesVar() + "," +
						stageRecord.getShuffleWriteBytesVar() + "," +
						stageRecord.getShuffleFetchWaitTimeVar() + "," +
						stageRecord.getShuffleWriteTimeVar() + "," +
						stageRecord.getShuffleReadRemoteBlocksVar() + "," +
						stageRecord.getTasksDeserialTimeVar() + "," +
						stageRecord.getTasksSerializeTimeVar() + "," +
						stageRecord.getTasksRunTimeVar() + "," +
						stageRecord.getTasksGCTimeVar() + "," +
						stageRecord.getTasksLastTimeVar() + "," ;

				startTimeStamp = startTimeStamp/1000;
				endTimeStamp = endTimeStamp/1000 + 1;
				StageGangliaInfo stageGangliaInfo = new StageGangliaInfo(startTimeStamp,endTimeStamp);
				stageGangliaInfo.queryInfo();

				line += stageGangliaInfo.getReadBytes()+"," +
						stageGangliaInfo.getWriteBytes() + "," +
						stageGangliaInfo.getBytesIn() + "," +
						stageGangliaInfo.getBytesOut() + "," +
						stageGangliaInfo.getCpuUtiPara() + "," +
						stageGangliaInfo.getDiskUtiPara() + "," +
						stageGangliaInfo.getNetUtiPara() + "," +
						stageGangliaInfo.getCpuUtiPara_1() + "," +
						stageGangliaInfo.getDiskUtiPara_1() + "," +
						stageGangliaInfo.getNetUtiPara_1() + "," +
						stageGangliaInfo.getCount()+ '\n';

				fw.write(line);
			}
		}
		fw.close();
	}

	public static void main(String[] args) throws IOException {
		String dirPath =args[0]; // "C:\\Users\\IBM_ADMIN\\IdeaProjects\\sparkWorkload\\history";//
		String output =args[1];  //"C:\\Users\\IBM_ADMIN\\IdeaProjects\\sparkLogParser\\bin\\workloadLog_3.17__.csv";
		//args[1];   //
		LocalParser lp = new LocalParser();
		List<String> apps =lp.listApps(dirPath);

		lp.localParser(dirPath,output,apps);

	}
}
