package hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
*使用原始数据对推荐结果集去重
* 	得到最终的推荐矩阵
*
*
*   10001,1 164283
*   10001,6 160597
*   10001,7 162850
*   10001,9 158876
*   10001,2 162387
*   10001,4 161892
*   10001,5 154079
*   10001,64 163716
*   10001,66 161587
* */
public class g_RV3 extends Configured implements Tool {

	static class rv31Mapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String k = value.toString().split("[,]")[0];
			context.write(new Text(k), new Text("r"+value.toString()));
		}
	}
	static class rv32Mapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[]data = value.toString().split("[ ]");
			context.write(new Text(data[0]), new Text("u"+data[0]+","+data[1]));
		}
	}
	static class rv3Reducer extends Reducer<Text, Text,Text, NullWritable> {

		private NullWritable v = NullWritable.get();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Map<String, String>r=new HashMap<>();
			List<String>u=new ArrayList<>();
			for(Text i:values){
				if(i.toString().startsWith("r")){
					String[] d = i.toString().substring(1).split("[ ]");
					r.put(d[0], d[1]);
				}else {
					u.add(i.toString().substring(1));
				}

			}
			for(String i:u){
				r.remove(i);
			}
			for(String i:r.keySet()){
				context.write(new Text(i+" "+r.get(i)), v);
			}
		}

	}

	@Override
	public int run(String[] arg0) throws Exception {
		//获得程序运行时的配置信息
		Configuration conf = getConf();
		Path input_rv = new Path(conf.get("input_rv"));
		Path input_uv = new Path(conf.get("input_uv"));
		Path output = new Path(conf.get("output"));
		// 构建新的作业
		Job job = Job.getInstance(conf);
		job.setJarByClass(this.getClass());
		job.setJobName("hadoop");
		// 给job设置mapper类及map方法输出的键值类型
		MultipleInputs.addInputPath(job, input_rv, TextInputFormat.class, rv31Mapper.class);
		MultipleInputs.addInputPath(job, input_uv, TextInputFormat.class, rv32Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// 给job设置reducer类及reduce方法输出的键值类型
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setReducerClass(rv3Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		// 设置数据的读取方式（文本文件）及结果的输出方式（文本文件）
		TextOutputFormat.setOutputPath(job, output);
		// 将作业提交集群执行
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new g_RV3(), args));
	}

}
