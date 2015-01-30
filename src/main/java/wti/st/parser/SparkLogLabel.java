package wti.st.parser;

public class SparkLogLabel
{
    public static String APPLICATION_COMPLETE_FILE = "APPLICATION_COMPLETE";
    public static String EVENT_LOG_1_FILE = "EVENT_LOG_1";
    
    // key
    public static String EVENT = "Event";

    public static String STAGE_ID = "Stage ID";
    public static String STAGE_ATTEMT_ID = "Stage Attempt ID";
    public static String STAGE_NAME = "Stage Name";
    public static String STAGE_DETAIL= "Details";
    public static String STAGE_TASK_NUM= "Number of Tasks";
    public static String STAGE_START_TIME= "Submission Time";
    public static String STAGE_END_TIME= "Completion Time";

    public static String RDD_INFO= "RDD Info";
    public static String RDD_ID= "RDD ID";
    public static String RDD_NAME= "Name";
    public static String RDD_STORAGE_LEVEL= "Storage Level";
    public static String RDD_PART_NUM= "Number of Partitions";
    public static String RDD_CACHED_PART_NUM= "Number of Cached Partitions";
    public static String RDD_MEM_SIZE= "Memory Size";
    public static String RDD_TACHYON_SIZE= "Tachyon Size";
    public static String RDD_DISK_SIZE= "Disk Size";

    public static String TASK_INFO = "Task Info";
    
    public static String TASK_ID = "Task ID";
    public static String TASK_ATTEMT_ID = "Attempt";
    public static String TASK_INDEX = "Index";
    public static String TASK_START_TIME = "Launch Time";
    public static String TASK_END_TIME = "Finish Time";
    public static String TASK_NODE = "Host";
    public static String TASK_LOCALITY = "Locality";

    public static String TASK_METRICS = "Task Metrics";
    public static String TASK_DESERIALIZE_TIME = "Executor Deserialize Time";
    public static String TASK_SERIALIZE = "Result Serialization Time";
    public static String TASK_RUN_TIME = "Executor Run Time";
    public static String TASK_GC_TIME = "JVM GC Time";
    public static String TASK_INPUT_INFO= "Input Metrics";
    public static String TASK_INPUT_FROM= "Data Read Method";
    public static String TASK_BYTES_READ= "Bytes Read";
    public static String TASK_RESULT_SIZE= "Result Size";
    public static String TASK_MEM_SPILLED= "Memory Bytes Spilled";
    public static String TASK_DISK_SPILLED= "Disk Bytes Spilled";


    public static String APP_NAME = "App Name";
    public static String TIMESTAMP = "Timestamp";
    
    
    //value
    public static String APP_START_EVENT = "SparkListenerApplicationStart";
    public static String APP_END_EVENT = "SparkListenerApplicationEnd";
    public static String STAGE_SUBMIT_EVENT = "SparkListenerStageSubmitted";
    public static String STAGE_COMPLETE_EVENT = "SparkListenerStageCompleted";
    public static String TASK_START_EVENT = "SparkListenerTaskStart";
    public static String TASK_END_EVENT = "SparkListenerTaskEnd";
    
   

    
}
