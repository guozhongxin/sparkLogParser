package wti.st.parser;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import wti.st.info.StageRecord;
import wti.st.info.TaskRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Deprecated
public class SparkTaskParser {
    private final String sparkLogDir;

    private String appID;

    public SparkTaskParser(String sparkLogDir) throws IOException {

        Configuration conf = new Configuration();
        URI uri = URI.create(sparkLogDir);
        FileSystem fs = FileSystem.get(uri, conf);

        if (!(fs.exists(new Path(sparkLogDir)))) {
            // XXX
            System.out.println("Path of the spark log directory is wrong");
        }
        if (!sparkLogDir.endsWith(File.separator)) {
            sparkLogDir += File.separator;
        }
        this.sparkLogDir = sparkLogDir;
    }

    public List<TaskRecord> taskParser(String appID) throws IOException {

        List<TaskRecord> tasks = new ArrayList<TaskRecord>();
        this.appID = appID;
        String appLogDir = sparkLogDir + appID + File.separator;

        String appLogFile = appLogDir + SparkLogLabel.EVENT_LOG_1_FILE;

        Configuration conf = new Configuration();
        URI uri = URI.create(appLogDir.toString());
        FileSystem fs = FileSystem.get(uri, conf);

        FSDataInputStream in = fs.open(new Path(appLogFile));
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String line = null;
        Gson gson = new Gson();
        while ((line = br.readLine()) != null) {
            Map<String, Object> lineMap = gson.fromJson(line,
                    new TypeToken<Map<String, Object>>() {
                    }.getType());
            String eventName = (String) lineMap.get(SparkLogLabel.EVENT);
            if (eventName.equals(SparkLogLabel.TASK_END_EVENT)) {
                Object stageIDObj = lineMap.get(SparkLogLabel.STAGE_ID);
                Object stageAttIDObj = lineMap
                        .get(SparkLogLabel.STAGE_ATTEMT_ID);

                int stageID = parserID(stageIDObj);
                int stageAttemprID = parserID(stageAttIDObj);

                StageRecord stage = new StageRecord(stageID,
                        stageAttemprID, this.appID);

                Object taskInfo = lineMap.get(SparkLogLabel.TASK_INFO);

                String taskInfoString = gson.toJson(taskInfo);

                Map<String, Object> taskMap = gson.fromJson(taskInfoString,
                        new TypeToken<Map<String, Object>>() {
                        }.getType());


                String nodeName = taskMap.get(SparkLogLabel.TASK_NODE)
                        .toString();

                // String index = taskMap.get(SparkLogLabel.TASK_INDEX);

                int taskID = parserID(taskMap.get(SparkLogLabel.TASK_ID));
                int taskAttID = parserID(taskMap
                        .get(SparkLogLabel.TASK_ATTEMT_ID));
                Timestamp startTime = parserTimestamp(
                        taskMap.get(SparkLogLabel.TASK_START_TIME));
                Timestamp endTime = parserTimestamp(
                        taskMap.get(SparkLogLabel.TASK_END_TIME));

                TaskRecord task = new TaskRecord(taskID, taskAttID,
                        stage);

                tasks.add(task);
            }
        }
        br.close();
        in.close();
        return tasks;
    }

    private int parserID(Object obj) {

        if (obj instanceof Double) {
            return (int) Math.round((Double) obj);
        } else if (obj instanceof String) {
            Object object = Double.parseDouble((String) obj);
            return parserID(object);
        } else {
            return -1;
        }
    }

    private Timestamp parserTimestamp(Object object) {
        if (object instanceof Double) {
            return new Timestamp(Math.round((Double) object));
        } else {
            return new Timestamp(0l);
        }
    }

    public static void main(String[] args) throws IOException {
        SparkTaskParser taskParser = new SparkTaskParser(
                "hdfs://bigant-test-001:8020/spark110log");
        List<TaskRecord> tasks = taskParser.taskParser("wc-1412857997167");
        for (TaskRecord sparkTaskRecord : tasks) {
            System.out.println(sparkTaskRecord);
        }
//        List<Integer> l = new ArrayList<Integer>();
//        for (int i =0;i<10;i++){
//            l.add(i);
//        }
//        for (int i = 0;i<l.size();i++){
//            int p = l.get(i)*10;
//            l.set(i,p);
//        }
//        System.out.print(l);
    }
}
