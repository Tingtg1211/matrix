package step2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MR2 {
      // 输入文件相对路径
//    private static String inPath = "/matrix/step2_input/matrix1.txt";
//    // 输出文件相对路径
//    private static String outPath = "/matrix/output";
      // 将step1输出的转置矩阵作为全局缓存
//      private static String cache = "/matrix/step1_output";
    // hdfs路径
//    private static String hdfs = "hdfs://localhost:9000";

    public int run(String[] args) {

        try {
            // 创建job配置类
            Configuration conf = new Configuration();
//            // 设置hdfs地址
//            conf.set("fs.defaultFS", hdfs);
            // 创建一个job实例
            Job job = Job.getInstance(conf, "step2");
            // 添加分布式缓存文件

//            job.addCacheArchive(new URI(args[2] + "#matrix2"));
            job.addCacheFile(new URI(args[2] + "#matrix2"));


            // 设置job的主类
            job.setJarByClass(MR2.class);
            // 设置job的Mapper类和Reducer类
            job.setMapperClass(Mapper2.class);
            job.setReducerClass(Reducer2.class);

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
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return -1;

    }

    public static void main(String[] args) {
        int result = -1;
        result = new MR2().run(args);
        if(result == 1) {
            System.out.println("step2运行成功。。。");

        }else if(result == -1) {
            System.out.println("step2运行失败。");
        }
    }


}
