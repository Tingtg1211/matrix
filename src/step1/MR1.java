package step1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

public class MR1 {
//    // 输入文件相对路径
//    private static String inPath = "/matrix/step1_input/matrix2.txt";
//    // 输出文件相对路径
//    private static String outPath = "/matrix/step1_output";
    // hdfs路径
//    private static String hdfs = "hdfs://localhost:9000";

    public int run(String[] args) {

        try {
            // 创建job配置类
            Configuration conf = new Configuration();
//            // 设置hdfs地址
//            conf.set("fs.defaultFS", hdfs);
            // 创建一个job实例
            Job job = Job.getInstance(conf, "step1");

            // 设置job的主类
            job.setJarByClass(MR1.class);
            // 设置job的Mapper类和Reducer类
            job.setMapperClass(Mapper1.class);
            job.setReducerClass(Reducer1.class);

            // 设置Mapper输出类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            // 设置Reducer输出类型
            job.setOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            FileSystem fs = FileSystem.get(conf);
            // 设置输入和输出路径
            Path inputPath = new Path(args[0]);
            if(fs.exists(inputPath)) {
                FileInputFormat.addInputPath(job, inputPath);

            }

            Path outputPath = new Path(args[1]);
            fs.delete(outputPath, true);
            FileOutputFormat.setOutputPath(job, outputPath);


            return job.waitForCompletion(true) ? 1 : -1;

        } catch (InterruptedException e) {
                e.printStackTrace();
        } catch (ClassNotFoundException e) {
                e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;

    }

    public static void main(String[] args) {
        int result = -1;
        result = new MR1().run(args);
        if(result == 1) {
            System.out.println("step1运行成功。。。");

        }else if(result == -1) {
            System.out.println("step1运行失败。");
        }
    }
}
