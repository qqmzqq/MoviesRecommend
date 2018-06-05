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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * 计算出推荐矩阵（相似度矩阵 X 向量）
 *  电影相似度矩阵 x 用户观影偏好值 = 推荐向量
 *
 * 	 1 [10001:5,10004:7,11234:9, ... ]       1 [100:1718,11:1703,12:1694, 13:1703,  ... ]
 *   2 [10004:1,10005:9,10002:0, ... ]   X   2 [54:1667, 1:1664, 10:3647, 100:1667, ... ]
 *   3 [10001:3,20000:7,19998:3, ... ]       3 [1:1718,  10:1667,100:3714,11:1724,  ... ]
 *
 * 得到:
 *   10249,100 17180
 *   14920,100 10308
 *   12879,100 15462
 *   10009,100 10308
 *   12018,100 6872
 *   10673,100 8590
 *   16259,100 1718
 *   18571,100 10308
 *   19794,100 12026
 *   18818,100 13744
 *   17153,100 0
 *   18974,100 10308
 *   10674,100 15462
 *   12015,100 17180
 *   ...
 *
 * */
public class e_RV1 extends Configured implements Tool {

	static class rv11Mapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] datas = value.toString().split("[ ]");
			context.write(new Text(datas[0]), new Text("m"+datas[1]));
		}
	}
	static class rv12Mapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] datas = value.toString().split("[ ]");
			context.write(new Text(datas[0]), new Text("u"+datas[1]));
		}
	}
	static class rv1Reducer extends Reducer<Text, Text,Text, NullWritable> {

		private NullWritable v = NullWritable.get();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String[] movies = null;
			String[] users = null;
			for(Text i:values){
				// 将两个矩阵分别存到对应数组
				if(i.toString().startsWith("m")){
					movies = i.toString().substring(1).split("[,]");
				}
				else users = i.toString().substring(1).split("[,]");
			}
			// 两个矩阵的相似度相乘
			for(String m: movies){
				for(String u: users){
					String[] movie = m.split("[:]");
					String[] user = u.split("[:]");
					context.write(new Text(user[0]+","+movie[0]+" "+(Integer.parseInt(movie[1])*Integer.parseInt(user[1]))), v);
				}
			}

		}

	}

	@Override
	public int run(String[] arg0) throws Exception {
		// 获得程序运行时的配置信息
		Configuration conf = getConf();
		Path input_sm = new Path(conf.get("input_sm"));
		Path input_pv = new Path(conf.get("input_pv"));
		Path output = new Path(conf.get("output"));
		// 构建新的作业
		Job job = Job.getInstance(conf);
		job.setJarByClass(e_RV1.class);
		job.setJobName("hadoop");
		// 给job设置mapper类及map方法输出的键值类型
		MultipleInputs.addInputPath(job, input_sm, TextInputFormat.class, rv11Mapper.class);
		MultipleInputs.addInputPath(job, input_pv, TextInputFormat.class, rv12Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// 给job设置reducer类及reduce方法输出的键值类型
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setReducerClass(rv1Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		// 设置数据的读取方式（文本文件）及结果的输出方式（文本文件）
		TextOutputFormat.setOutputPath(job, output);
		// 将作业提交集群执行
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new e_RV1(), args));
	}

}
