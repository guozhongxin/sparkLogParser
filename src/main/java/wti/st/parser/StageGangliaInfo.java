package wti.st.parser;

import com.ibm.bigant.catalog.*;
import com.ibm.bigant.catalog.dao.Dao;
import com.ibm.bigant.catalog.dao.DaoManager;
import com.ibm.bigant.catalog.profile.ganglia.GangliaCPUProfiler;
import com.ibm.bigant.catalog.profile.test.TestHelper;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.*;

/**
 * Created by dell on 2015/11/18.
 */
public class StageGangliaInfo {

    private long startTimestamp = 0l;
    private long endTimestamp = 0l;

    private static final Dao dao = Dao.getDao(DaoManager.getPMF("bigant-mysql")
            .getPersistenceManager());

    private long readBytes = 0l;
    private long writeBytes = 0l;
    private long bytesIn = 0l;
    private long bytesOut = 0l;

    private float cpuUtiPara = 0;
    private float diskUtiPara = 0;
    private float netUtiPara = 0;

    private int cpuUtiPara_1 = 0;
    private int diskUtiPara_1 = 0;
    private int netUtiPara_1 = 0;

    private int count = 0;

    StageGangliaInfo(long startTime, long endTime) {
        this.startTimestamp = startTime;
        this.endTimestamp = endTime;
    }

    public void queryInfo() throws IOException{
        Collection<GangliaDiskState> ggDisks = dao.findGangliaPeriod(GangliaDiskState.class, startTimestamp, endTimestamp);
        Collection<GangliaNet> ggNets = dao.findGangliaPeriod(GangliaNet.class, startTimestamp, endTimestamp);
        Collection<GangliaCPU> ggCpus = dao.findGangliaPeriod(GangliaCPU.class, startTimestamp, endTimestamp);

        FileWriter diskStateFW = new FileWriter("./stagesGanglia/"+startTimestamp+"-ggDiskStates.csv");
        String dsFirstLine = "timeStamp,node,diskname,"
                + "diskstat_sd*_percent_io_time,"
                + "diskstat_sd*_write_bytes_per_sec,"
                + "diskstat_sd*_read_bytes_per_sec" + "\n";
        diskStateFW.write(dsFirstLine);
        for (GangliaDiskState ggDisk : ggDisks)
        {
            String line = ggDisk.getTimeStamp() + ","
                    + ggDisk.getNodeId().getName() + ","
                    + ggDisk.getDiskName() + ","
                    + ggDisk.getIoTimePercent() + ","
                    + ggDisk.getWriteBytesPerSecond() + ","
                    + ggDisk.getReadBytesPerSecond() +"\n";
            diskStateFW.write(line);
        }
        diskStateFW.flush();

        FileWriter cpuFW = new FileWriter("./stagesGanglia/"+startTimestamp+"-ggCpu.csv");
        String ggCpuFirstLine = "timeStamp,node,user,system,wio,aidle,idle,nice"
                + "\n";
        cpuFW.write(ggCpuFirstLine);
        for (GangliaCPU ggCPU : ggCpus)
        {
            String line = ggCPU.getTimeStamp() + ","
                    + ggCPU.getNodeId().getName() + "," + ggCPU.getCpuUser()
                    + "," + ggCPU.getCpuSystem() + "," + ggCPU.getCpuWio()
                    + "," + ggCPU.getCpuAidle() + "," + ggCPU.getCpuIdle()
                    + "," + ggCPU.getCpuNice() + "\n";
            cpuFW.write(line);
        }
        cpuFW.flush();

        FileWriter netFW = new FileWriter("./stagesGanglia/"+startTimestamp+"-ggCpu.csv");
        String ggNetFirstLine = "timeStamp,node,bytesIn,bytesOut" + "\n";
        netFW.write(ggNetFirstLine);
        for (GangliaNet ggNet : ggNets)
        {
            String line = ggNet.getTimeStamp() + "," + ggNet.getNodeId().getName() + ","
                    + ggNet.getBytesIn()+"," + ggNet.getBytesOut() + "\n";
            cpuFW.write(line);
        }
        cpuFW.flush();
        getAmount(ggDisks, ggNets);
        getUtilization(ggCpus,ggDisks,ggNets);
    }

    /**
     * get diskIO & networkIO amount
     */
    public void getAmount(Collection<GangliaDiskState> ggDisks, Collection<GangliaNet> ggNets) {

        for (GangliaDiskState ggDisk : ggDisks){
            if (ggDisk.getDiskName().equals("sdb") && !ggDisk.getNodeId().getName().equals("dell01.wtist")){
                this.readBytes += ggDisk.getReadBytesPerSecond();
                this.writeBytes += ggDisk.getWriteBytesPerSecond();
            }
        }

        for (GangliaNet ggNet : ggNets){
            if(!ggNet.getNodeId().getName().equals("dell01.wtist")){
                this.bytesIn += ggNet.getBytesIn();
                this.bytesOut += ggNet.getBytesOut();
            }
        }
    }

    /**
     * analysis of utilization of cpu/diskIO/networkIO
     */
    public void getUtilization(Collection<GangliaCPU> ggCpus, Collection<GangliaDiskState> ggDisks, Collection<GangliaNet> ggNets) {

        Map<String,Long> diskstateMap = new HashMap<String, Long>();

        int utiSum =0;
        int tmp = 0;

        for (GangliaCPU ggCpu : ggCpus){
            tmp = tmp%6+1;
            if (!ggCpu.getNodeId().getName().equals("dell01.wtist")){
                if (ggCpu.getCpuUser()>=80){
                    cpuUtiPara++;
                }
                utiSum+=ggCpu.getCpuUser();
            }
            if (tmp==6 ){
                if (utiSum >= 320) {
                    cpuUtiPara_1 += 1;
                }
                utiSum=0;
            }
        }

        cpuUtiPara=cpuUtiPara/4;

        utiSum =0;
        tmp = 0;
        for (GangliaDiskState ggDisk : ggDisks){
            tmp = tmp%12 + 1;
            if (ggDisk.getDiskName().equals("sdb") &&!ggDisk.getNodeId().getName().equals("dell01.wtist")){
                String key = ggDisk.getNodeId().getName()+ggDisk.getTimeStamp();
                diskstateMap.put(key, ggDisk.getWriteBytesPerSecond()+ggDisk.getReadBytesPerSecond());
                if (ggDisk.getIoTimePercent()>=80){
                    diskUtiPara++;
                }
                utiSum+=ggDisk.getIoTimePercent();
            }
            if (tmp==12 ){
                if (utiSum >= 320) {
                    diskUtiPara_1 += 1;
                }
                utiSum=0;
            }
        }
        diskUtiPara=diskUtiPara/4;

        utiSum =0;
        tmp = 0;
        for (GangliaNet ggNet : ggNets){
            tmp = tmp%12 + 1;
            if (!ggNet.getNodeId().getName().equals("dell01.wtist")){
                String key = ggNet.getNodeId().getName()+ggNet.getTimeStamp();
                long disk = diskstateMap.get(key);

                double networkAmount = (ggNet.getBytesIn() + ggNet.getBytesOut() - disk)/1024/1024;
                if (networkAmount > 45){
                    netUtiPara++;
                }
                utiSum+=networkAmount;
            }
            if (tmp==12 ){
                if (utiSum >= 180) {
                    diskUtiPara_1 += 1;
                }
                utiSum=0;
            }
        }
        netUtiPara=netUtiPara/4;
    }

    @Override
    public String toString() {
        return "StageGangliaInfo{" +
                "\nstartTimestamp=" + startTimestamp +
                ", \nendTimestamp=" + endTimestamp +
                ", \nreadBytes=" + readBytes +
                ", \nwriteBytes=" + writeBytes +
                ", \nbytesIn=" + bytesIn +
                ", \nbytesOut=" + bytesOut +
                ", \ncpuUtiPara=" + cpuUtiPara +
                ", \ndiskUtiPara=" + diskUtiPara +
                ", \nnetUtiPara=" + netUtiPara +
                ", \ncpuUtiPara_1=" + cpuUtiPara_1 +
                ", \ndiskUtiPara_1=" + diskUtiPara_1 +
                ", \nnetUtiPara_1=" + netUtiPara_1 +
                ", \ncount" + count +
                '}';
    }

    public long getReadBytes() {
        return readBytes;
    }

    public long getWriteBytes() {
        return writeBytes;
    }

    public long getBytesIn() {
        return bytesIn;
    }

    public long getBytesOut() {
        return bytesOut;
    }

    public float getCpuUtiPara() {
        return cpuUtiPara;
    }

    public float getDiskUtiPara() {
        return diskUtiPara;
    }

    public float getNetUtiPara() {
        return netUtiPara;
    }

    public int getCpuUtiPara_1() {
        return cpuUtiPara_1;
    }

    public int getDiskUtiPara_1() {
        return diskUtiPara_1;
    }

    public int getNetUtiPara_1() {
        return netUtiPara_1;
    }

    public int getCount() {
        return count;
    }

    public static void main(String[] args) throws IOException{
        long start =Long.parseLong(args[0]);
        long end = Long.parseLong(args[1]);
        StageGangliaInfo stageGangliaInfo = new StageGangliaInfo(start,end);
        stageGangliaInfo.queryInfo();
        System.out.println(stageGangliaInfo);
    }
}
