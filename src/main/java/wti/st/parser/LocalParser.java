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
			if (appCompleteFile.exists()) {
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

		fw.write("stageID,stageAttID,appID,stageName,taskNum,runningTime,stageType,inputFromHadoop," +
				"inputFromMem,inputFromDisk,shuffleRead,shuffleWrite," +
				"shuffleReadLocalBlocks,shuffleReadRemoteBlocks,diskSpilled,memSpilled,resultSize," +
				"shuffleFetchWaitTime,shuffleWriteTime,tasksDeserialTime,tasksSerializeTime," +
				 "tasksRunTime,tasksGCTime,tasksLastTime\n");
		for (String appID : appIDs) {

			if (parseredAppsID.contains(appID)){
				continue;
			}

			SparkStageParser stageParser = new SparkStageParser(dir);

			String eventFile = dir + File.separator + appID + File.separator + SparkLogLabel.EVENT_LOG_1_FILE;
			System.out.println("## Parser appID:	" + appID);

			List<StageRecord> stages = stageParser.stageParserLocal(eventFile,appID);

			for (StageRecord stageRecord : stages) {
				Long stageRunningTime = stageRecord.getEndTime().getTime() -
						stageRecord.getSubmitTime().getTime();
				String line = stageRecord.getStageID() + "," +
						stageRecord.getStageAttemptID() + "," +
						stageRecord.getAppID() + "," +
						stageRecord.getStageName() + "," +
						stageRecord.getTaskNum() + "," +
						stageRunningTime + "," +
						stageRecord.getStageType() + "," +
						stageRecord.getInputFromHadoop() + "," +
						stageRecord.getInputFromMem() + "," +
						stageRecord.getInputFromDisk() + "," +
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
						stageRecord.getTasksLastTime() +"\n";
				fw.write(line);
			}
		}
		fw.close();
	}

	public static void main(String[] args) throws IOException {
		String dirPath = "C:\\Users\\IBM_ADMIN\\IdeaProjects\\sparkWorkload\\history";//args[0]; //
		String output ="C:\\Users\\IBM_ADMIN\\IdeaProjects\\sparkLogParser\\bin\\workloadLog_3.17__.csv";
		//args[1];   //
		LocalParser lp = new LocalParser();
		List<String> apps =lp.listApps(dirPath);

		lp.localParser(dirPath,output,apps);

	}
}
