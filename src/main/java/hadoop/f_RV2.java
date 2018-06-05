package hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
* 对上一步的结果进行求和
*
*   10001,1 164283
*   10001,10 160059
*   10001,100 162566
*   10001,11 164015
*   10001,12 159836
*   10001,13 161262
*   10001,14 179475
*   10001,15 159397
*   10001,16 176527
*   10001,17 161882
*   10001,18 172132
*   10001,19 163831
*   10001,2 162387
*   ...
*
* */
public class f_RV2 extends Configured implements Tool{
	static class rv2Mapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		@Override
		protected void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
			String[] datas = value.toString().split("[ ]");
			context.write(new Text(datas[0]), new IntWritable(Integer.parseInt(datas[1])));
		}
	}
	static class rv2Reducer extends Reducer<Text,IntWritable,Text,NullWritable>{
		private NullWritable v = NullWritable.get();
		@Override
		protected void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
			int sum = 0;
			for(IntWritable i:values){
				sum = sum+i.get();
			}
			context.write(new Text(key.toString()+" "+sum), v);
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
				job.setJarByClass(f_RV2.class);
				// 给job设置mapper类及map方法输出的键值类型
				job.setMapperClass(rv2Mapper.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(IntWritable.class);
				// 给job设置reducer类及reduce方法输出的键值类型
				job.setReducerClass(rv2Reducer.class);
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
		int status=ToolRunner.run(new f_RV2(),args);
		System.exit(status);
	}
}
