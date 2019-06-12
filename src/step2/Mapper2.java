package step2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class Mapper2 extends Mapper<LongWritable, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();
    private List<String > cacheList = new ArrayList<>();

    // 初始化方法在map方法执行之前执行且执行一次
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        // 通过输入流将全局缓存中的右侧矩阵读入list<String>中
//        FileReader fr = new FileReader("matrix2");
//        BufferedReader br = new BufferedReader(fr);

        URI[] cacheFiles = context.getCacheFiles();
        Path cacheFile = new Path(cacheFiles[0]);
        System.out.println("分布式缓存"+cacheFile);
        System.out.println("分布式缓存路径"+cacheFile.toUri().getPath());
        //DistributedCache.getLocalCacheFiles(conf)
        //BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("pdts.txt")));
        //BufferedReader br = new BufferedReader(new FileReader("pdts.txt"));
        BufferedReader br = new BufferedReader(new FileReader(cacheFile.toUri().getPath()));



        // 每一行的格式是: 行 tab 列_值，列_值，列_值，列_值
        String line = null;
        while ((line = br.readLine()) != null) {
            cacheList.add(line);
        }
//        fr.close();
        br.close();

    }

    /**
     *
     * @param key：行号
     * @param value：行 tab 列_值，列_值，列_值，列_值
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 行
        String row_matrix1 = value.toString().split("\t")[0];
        // 列_值 (数组)
        String[] colum_value_array_matrix1 = value.toString().split("\t")[1].split(",");

        for(String line: cacheList) {
            // 右侧矩阵的行 line
            // 格式：行 tab 列_值，列_值，列_值，列_值
            String row_matrix2 = line.toString().split("\t")[0];
            String[] colum_value_array_matrix2 = line.toString().split("\t")[1].split(",");

            // 矩阵两行相乘得到的结果
            int result = 0;
            // 遍历左矩阵的第一行的每一列
            for (String column_value_matrix1: colum_value_array_matrix1) {
                String column_matrix1 = column_value_matrix1.split("_")[0];
                String value_matrix1 = column_value_matrix1.split("_")[1];

                // 遍历右矩阵的每一行的每一列
                for (String column_value_matrix2: colum_value_array_matrix2) {
                    if (column_value_matrix2.startsWith(column_matrix1 + "_")) {
                        String value_matrix2 = column_value_matrix2.split("_")[1];
                        // 将两列的值相乘并累加
                        result += Integer.valueOf(value_matrix1) * Integer.valueOf(value_matrix2);
                    }

                }
            }
            // result是结果矩阵中的某元素，坐标为 行： row_matrix1， 列： row_matrix2(因为右矩阵已经转置）
            outKey.set(row_matrix1);
            outValue.set(row_matrix2 + "_" + result);
            // 输出格式：key：行  value：列_值
            context.write(outKey, outValue);

        }


    }

}
