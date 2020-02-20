import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class WordCount {

    /**
     * spark2.x统一编程入口，可以根据需求再获取sparkContext。
     * 但好像就不支持javaSparkContext了，要单独创建对象
     * @return SparkSession
     */
    public static SparkSession getSparkSession(){
        return SparkSession.builder()
                .appName("WordCount")
                .master("local[4]").getOrCreate();
    }

    /**
     * 通过javaSparkContext获取编程入口
     */
    public static JavaSparkContext getJavaSparkContext(){
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[4]");
        return new JavaSparkContext(conf);
    }
    //用于wordCount1,wordCount2
    private static JavaRDD<String> getData(){
        List<String> strings = Arrays.asList("hello hi hi hello", "jack jay tom jack", "jack tom hello hi jay");
        JavaSparkContext jsc = getJavaSparkContext();

        int defaultParallelism = jsc.defaultParallelism();
        //local模式：默认和指定相同
        System.out.println("spark默认默认并行度：" + defaultParallelism);

        return jsc.parallelize(strings);
    }

    /**
     * rdd的编程方式, 算子接收匿名内部类
     * 代码相对冗长，而且每个匿名内部类实现的接口有一定差异，不方便记忆
     */
    public static void wordCount1(){
        JavaRDD<String> rdd1 = getData();
        //切分压平，配对
        JavaPairRDD<String, Integer> rdd2_1 = rdd1.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(String line) throws Exception {
                String[] strings = line.split(" ");
                ArrayList<Tuple2<String, Integer>> list = new ArrayList<>();
                for (String string : strings) {
                    list.add(new Tuple2<>(string, 1));
                }
                return list.iterator();
            }
        });
        //切分压平，配对分开执行计划
        JavaPairRDD<String, Integer> rdd2_2 = rdd1.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });

        JavaPairRDD<String, Integer> rdd3 = rdd2_1.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        List<Tuple2<String, Integer>> collect = rdd3.collect();
        collect.forEach(System.out::println);

    }

    /**
     * 使用lambda表达式，基本可以做到和scala一样精简，而且大环境依然是java，对于java程序员也相对是友好的
     * 我认为是个比较好的设计
     */
    public static void wordCount2(){
        JavaRDD<String> rdd1 = getData();
        //切分压平，配对
        JavaPairRDD<String, Integer> rdd2_1 = rdd1.flatMapToPair( line -> {
            String[] strings = line.split(" ");
            ArrayList<Tuple2<String, Integer>> list = new ArrayList<>();
            for (String string : strings) {
                list.add(new Tuple2<>(string, 1));
            }
            return list.iterator();
        });
        //切分压平，配对分开执行计划
        JavaPairRDD<String, Integer> rdd2_2 = rdd1.flatMap(line -> Arrays.asList(line.split(" ")).iterator()).mapToPair(word -> new Tuple2<>(word, 1));
        //这一句精简的很是到位
        JavaPairRDD<String, Integer> rdd3 = rdd2_1.reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum);
        List<Tuple2<String, Integer>> collect = rdd3.collect();
        collect.forEach(System.out::println);
    }

    /**
     * 使用SparkSession入口 和 DateSet api
     * 我是打算写来着，但是发现没法写，和rdd的function接口不一样好像，看来只能用scala了么，或者用hive sql语法来写了
     */
    public static void wordCount3(){
        SparkSession sparkSession = getSparkSession();
        Dataset<String> ds1 = sparkSession.read().textFile("rddData/wordCount.txt");
        Dataset<Row> rowDataset = ds1.toDF();
    }

    public static void main(String[] args) {
        WordCount.wordCount1();
    }
}
