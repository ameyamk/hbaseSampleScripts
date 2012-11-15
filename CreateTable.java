import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Created by IntelliJ IDEA.
 * User: ameya
 * Date: 10/25/12
 * Time: 3:48 PM
 * To change this template use File | Settings | File Templates.
 */
public class CreateTable {

    public static void createHbaseTable(String tableName) {
        Configuration hbaseConfig;
        String hbaseZookeeperQuorum = "zk1.east,zk2.east,zk3.east";
        String hbaseZookeeperClientPort = "2181";
        String host = "10.20.73.56";
        String hdfsPort = "8020";
        String hbaseTable = tableName;
        String zooParent = "/hbase";
        HBaseAdmin admin;
        HTable userTable;
        HColumnDescriptor colDesc;
        hbaseConfig = HBaseConfiguration.create();

        hbaseConfig.set("hbase.rootdir", "hdfs://" + host + ":" + hdfsPort + "/hbase");
        hbaseConfig.set("hbase.cluster.distributed", "true");
        hbaseConfig.set("hbase.zookeeper.quorum", hbaseZookeeperQuorum);
        hbaseConfig.set("hbase.zookeeper.property.clientPort", String.valueOf(hbaseZookeeperClientPort));
        hbaseConfig.set("zookeeper.znode.parent", zooParent);
        try {
            admin = new HBaseAdmin(hbaseConfig);
            if (!admin.tableExists(hbaseTable)) {
                System.out.println("Could not find HBase table " + hbaseTable + ", creating now");
                HTableDescriptor desc = new HTableDescriptor();
                desc.setName(hbaseTable.getBytes());
                colDesc = new HColumnDescriptor("cf1");
                colDesc.setCompressionType(Compression.Algorithm.SNAPPY);
                desc.addFamily(colDesc);
                byte[][] splits = getSplits(1, 100000, 100);
                admin.createTable(desc, splits);
            }
            userTable = new HTable(hbaseConfig, hbaseTable);
            System.out.println("Table created successfully");
        } catch (Exception e) {
            throw new RuntimeException("Error while accessing hbase....");
        }
    }

    public static byte[][] getSplits(long min_id, long max_id, int chunks){
        long chunkSize = (max_id - min_id) / chunks;
        byte[][] splits = new byte[chunks+2][];
        int index = 0;
        for (Long i = min_id; i <= (max_id + chunkSize); i += chunkSize) {
            Long start = i;
            Long end = i + chunkSize - 1;
            splits[index] = (String.valueOf(end)).getBytes();
            System.out.println(start);
            index++;
        }

        return splits;
    }

    public static void main(String[] args) throws Exception {
        createHbaseTable(args[0]);
    }
}
