package wti.st.parser;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import wti.st.info.SparkJobRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SparkJobParser
{

    private final String sparkLogDir;

    public SparkJobParser(String sparkLogDir) throws IOException
    {
	Configuration conf = new Configuration();

	URI uri = URI.create(sparkLogDir);

	FileSystem fs = FileSystem.get(uri, conf);

	if (!( fs.exists(new Path(sparkLogDir)) ))
	{
	    // XXX
	    System.out.println("Path of the spark log directory is wrong");
	}
	this.sparkLogDir = sparkLogDir;

    }

    public List<SparkJobRecord> listJobs() throws IOException
    {

	List<SparkJobRecord> sparkJobRecords = new ArrayList<SparkJobRecord>();
	Configuration conf = new Configuration();

	FileSystem fs = FileSystem.get(URI.create(sparkLogDir), conf);

	FileStatus[] files = fs.listStatus(new Path(this.sparkLogDir));
	for (FileStatus file : files)
	{

	    String appLogDir = file.getPath().toString();
	    if (!appLogDir.endsWith(File.separator))
	    {
		appLogDir += File.separator;
	    }
	    String appCompletePath = appLogDir.toString()
		    + SparkLogLabel.APPLICATION_COMPLETE_FILE;

	    fs = FileSystem.get(URI.create(appCompletePath), conf);
	    if (!fs.exists(new Path(appCompletePath))) {
		continue;
	    }
		

	    String eventLog = appLogDir.toString()
		    + SparkLogLabel.EVENT_LOG_1_FILE;
	    if (fs.exists(new Path(eventLog)))
	    {
		// XXX
		System.out.println("fetch " + appLogDir);
	    }
	    else
	    {
		continue;
	    }

	    String[] tmp = appLogDir.split(File.separator);
	    String jobID = tmp[tmp.length-1];
	    String jobName = null;
	    Timestamp startTime = null;
	    Timestamp endTime = null;

	    FSDataInputStream in = fs.open(new Path(eventLog));

	    BufferedReader br = new BufferedReader(new InputStreamReader(in));
	    String line = null;
	    Gson gson = new Gson();
	    while (( line = br.readLine() ) != null)
	    {

		Map<String, Object> map = gson.fromJson(line,
		        new TypeToken<Map<String, Object>>()
		        {
		        }.getType());
		String eventName = (String) map.get(SparkLogLabel.EVENT);
		if (eventName.equals(SparkLogLabel.APP_START_EVENT))
		{
		    Object obj = map.get(SparkLogLabel.TIMESTAMP);
		    if (obj instanceof Double){
			startTime = new Timestamp(Math.round((Double) obj));
		    }
		   
		    jobName = (String) map.get(SparkLogLabel.APP_NAME);
		}
		else if (eventName.equals(SparkLogLabel.APP_END_EVENT))
		{
		    Object obj = map.get(SparkLogLabel.TIMESTAMP);
		    if (obj instanceof Double){
			endTime = new Timestamp(Math.round((Double) obj));
		    }
		}

		// parserTask(map);
	    }
	    br.close();
	    in.close();

	    SparkJobRecord sparkJobRecord = new SparkJobRecord(jobName, jobID,
		    startTime, endTime);
	    sparkJobRecords.add(sparkJobRecord);
	}
	return sparkJobRecords;

    }

    private void parserTask(Map<String, String> map)
    {
	// TODO Auto-generated method stub

    }

    public static void main(String[] args) throws IOException
    {
	SparkJobParser jobParser = new SparkJobParser(
	        "hdfs://bigant-test-001:8020/spark110log");
	List<SparkJobRecord> jobList = jobParser.listJobs();
	for (SparkJobRecord sparkJobRecord : jobList)
	{
	    System.out.println(sparkJobRecord);
	}
    }
}
