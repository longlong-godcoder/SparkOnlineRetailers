package testSessionAnalysis;

import com.alibaba.fastjson.JSONObject;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;
import scala.util.parsing.input.StreamReader;
import spark.session.UserVisitSessionAnalyzeSpark;
import testMockData.TestMockData;
import utils.DateUtils;
import utils.ParamUtils;

import java.io.*;
import java.util.Date;
import java.util.List;

public class TestUserVisitSessionAnalyzeSpark {

    /**
     * 测试 session粒度聚合方法aggregateBySession
     * 聚合后和user_info表关联
     */
    public static void testAggregateBySession(){
        TestMockData.testMockData();
        JavaSparkContext jsc = TestMockData.getJavaSparkContext();
        SQLContext sqlContext = TestMockData.getSqlContext();

        String jsonStr = "{'startDate':['2020-02-15'], 'endDate':['2020-02-16']}";
        JSONObject jsonObject = JSONObject.parseObject(jsonStr);
        //根据时间范围获取user_visit_action数据
        JavaRDD<Row> acitionRDDByDateRange = UserVisitSessionAnalyzeSpark.getActionRDDByDateRange(sqlContext, jsonObject);
        //map分离出sessionid
        JavaPairRDD<String, Row> sessionid2ActionRDD1 = UserVisitSessionAnalyzeSpark.getSessionId2ActionRDD1(acitionRDDByDateRange);
        //聚合关联user_info
        JavaPairRDD<String, String> stringStringJavaPairRDD = UserVisitSessionAnalyzeSpark.aggregateBySession(sqlContext, sessionid2ActionRDD1);

        List<Tuple2<String, String>> collect = stringStringJavaPairRDD.collect();
        collect.forEach(System.out::println);
    }


    public static void main(String[] args) {
        TestUserVisitSessionAnalyzeSpark.testAggregateBySession();

//        InputStream resourceAsStream = TestUserVisitSessionAnalyzeSpark.class.getClassLoader().getResourceAsStream("user_visit_action.txt");
//        InputStreamReader inputStreamReader = new InputStreamReader(resourceAsStream);
//        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
//        String line;
//        try {
//            while ((line = bufferedReader.readLine()) != null){
//                String actionTime = line.split(",")[4];
//                Date date = DateUtils.parseTime(actionTime);
//                System.out.println(date);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }


//        Date date = DateUtils.parseTime("2020-02-16 1:33:44");
//        System.out.println(date);
    }
}
