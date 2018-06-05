package hadoop;

import java.io.IOException;

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
* 生成 用户观影偏好向量(矩阵)
*
*  电影 [用户:偏好值]
*
* 	1 [10001:5,10004:7,11234:9, ... ]
*   2 [10004:1,10005:9,10002:0, ... ]
*   3 [10001:3,20000:7,19998:3, ... ]
*   ...
*
*
* */
public class a_PV extends Configured implements Tool{
	static class pvMapper extends Mapper<LongWritable,Text,Text,Text>{
		@Override
		protected void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
			String[] datas = value.toString().split("[ ]");
			context.write(new Text(datas[1]), new Text(datas[0]+":"+datas[2]));
		}
	}
	static class pvReducer extends Reducer<Text,Text,Text,NullWritable>{
		private NullWritable v = NullWritable.get();
		@Override
		protected void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
			String str="";
			for(Text i:values){
				str=str + ","+i.toString();
			}
			context.write(new Text(key.toString()+" "+str.substring(1)), v);
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
				job.setJarByClass(a_PV.class);
				// 给job设置mapper类及map方法输出的键值类型
				job.setMapperClass(pvMapper.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
				// 给job设置reducer类及reduce方法输出的键值类型
				job.setReducerClass(pvReducer.class);
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
		int status=ToolRunner.run(new a_PV(),args);
		System.exit(status);
	}
}
