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
* 对上一步求出的数据求和(以同一种共现为Key)
* 得出每种结果的贡献次数
*
*  1,1,3749
*  1,10,1664
*  1,100,1718
*  1,11,1703
*  1,12,1694
*  1,13,1703
*  1,14,1686
*  1,15,1671
*  1,16,1755
*  1,17,1736
*  1,18,1636
*  1,19,1689
*  1,2,1719
*  ...
*
* */

public class c_SM2 extends Configured implements Tool{
	static class sm2Mapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		@Override
		protected void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
			String[] datas = value.toString().split("[ ]");
			context.write(new Text(datas[0]), new IntWritable(Integer.parseInt(datas[1])));
		}
	}
	static class sm2Reducer extends Reducer<Text,IntWritable,Text,NullWritable>{
		private NullWritable v = NullWritable.get();
		@Override
		protected void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
			int sum = 0;
			for(IntWritable i:values){
				sum = sum+i.get();
			}
			context.write(new Text(key.toString()+","+sum), v);
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
				job.setJarByClass(c_SM2.class);
				// 给job设置mapper类及map方法输出的键值类型
				job.setMapperClass(sm2Mapper.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(IntWritable.class);
				// 给job设置reducer类及reduce方法输出的键值类型
				job.setReducerClass(sm2Reducer.class);
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
		int status=ToolRunner.run(new c_SM2(),args);
		System.exit(status);
	}
}
