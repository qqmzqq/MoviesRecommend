package hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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

/*
* 求共现矩阵(相似度矩阵)_1
* 通过原始数据 拿到 user id 和 index
* 以user id为Key 求 所有被用户评价过的电影出现的次数
*
* 	1,1 1
* 	1,1 1
* 	1,2 1
*   1.2 1
* 	1,2 1
*   ...
*
* */
public class b_SM1 extends Configured implements Tool{
	static class sm1Mapper extends Mapper<LongWritable,Text,Text,Text>{
		@Override
		protected void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
			String[] datas = value.toString().split("[ ]");
			context.write(new Text(datas[0]), new Text(datas[1]));
		}
	}
	static class sm1Reducer extends Reducer<Text,Text,Text,NullWritable>{
		private NullWritable v = NullWritable.get();
		@Override
		protected void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
			List<String> list = new ArrayList<>();
			for(Text one:values){
				list.add(one.toString());
			}
			for(String i:list){
				for(String j:list){
					context.write(new Text(i+","+j+" "+1),v);
				}
			}
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// 获得程序运行时的配置信息
				Configuration conf=this.getConf();
				String inputPath=conf.get("input");
				String outputPath=conf.get("output");
				// 构建新的作业
				Job job=Job.getInstance(conf,"hadoop");
				job.setJarByClass(b_SM1.class);
				// 给job设置mapper类及map方法输出的键值类型
				job.setMapperClass(sm1Mapper.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
				// 给job设置reducer类及reduce方法输出的键值类型
				job.setReducerClass(sm1Reducer.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(NullWritable.class);
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
		int status=ToolRunner.run(new b_SM1(),args);
		System.exit(status);
	}
}
