package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;


/*
* 对推荐结果集进行Top K 计算(K=10)
*
*    10001	25	166589
*    10001	43	165587
*    10001	59	165224
*    10001	87	164733
*    10001	49	164599
*    10001	37	164320
*    10001	1	164283
*    10001	63	164041
*    10001	30	164028
*    10001	11	164015
*    ...
*
* */
public class h_Top10 extends Configured implements Tool{
    private static final int K = 10;
    public static class h_Top10Mapper extends Mapper<LongWritable, Text, Text, Text>{
        Text outKey = new Text();
        Text outValue = new Text();
        String[] valArr = null;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            valArr = value.toString().split("[,]");
            outKey.set(new Text(valArr[0]));
            outValue.set(new Text(valArr[0] + " " +valArr[1]));
            context.write(outKey, outValue);
        }
    }

    public static class h_Top10Reducer extends Reducer<Text, Text, Text, Text>{
        Text outKey = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            TreeMap<Integer, String> treeMap = new TreeMap<>();
            TreeSet<Integer> treeSet = new TreeSet<>();
            for (Text value: values){
                String[] k_v = value.toString().split("[ ]");
                treeMap.put(Integer.parseInt(k_v[2]), k_v[0] + "\t" + k_v[1]);
                treeSet.add(Integer.parseInt(k_v[2]));
                if (treeSet.size() > K) {
                    treeSet.remove(treeSet.first());
                }
            }
            for (int i: treeSet.descendingSet()){
                outKey.set(treeMap.get(i));
                context.write(outKey, new Text(String.valueOf(i)));
            }

        }
    }

    @Override
    public int run(String[] args) throws Exception {
        //获得程序运行时的配置信息
        Configuration conf=this.getConf();
        String inputPath=conf.get("input");
        String outputPath=conf.get("output");
        // 构建新的作业
        Job job=Job.getInstance(conf,"hadoop");
        job.setJarByClass(h_Top10.class);
        // 给job设置mapper类及map方法输出的键值类型
        job.setMapperClass(h_Top10.h_Top10Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // 给job设置reducer类及reduce方法输出的键值类型
        job.setReducerClass(h_Top10.h_Top10Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // 设置数据的读取方式（文本文件）及结果的输出方式（文本文件）
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // 设置输入和输出目录
        FileInputFormat.addInputPath(job,new Path(inputPath));
        FileOutputFormat.setOutputPath(job,new Path(outputPath));
        // 将作业提交集群执行
        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        int status= ToolRunner.run(new h_Top10(),args);
        System.exit(status);
    }
}