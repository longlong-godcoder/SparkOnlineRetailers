package testSessionAnalysis;

import com.alibaba.fastjson.JSONObject;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import spark.session.SessionAggregateStatAccumulator;
import spark.session.UserVisitSessionAnalyzeSpark;
import testMockData.TestMockData;

import java.io.*;
import java.util.List;

public class TestUserVisitSessionAnalyzeSpark {

    private static JSONObject jsonObject;
    private static JavaSparkContext jsc;

    private static JavaPairRDD<String, Row> sessionId2ActionRDD1;
    /**
     * 测试 session粒度聚合方法aggregateBySession
     * 聚合后和user_info表关联
     */
    public static JavaPairRDD<String, String> testAggregateBySession(){
        TestMockData.testMockData();
        jsc = TestMockData.getJavaSparkContext();
        SQLContext sqlContext = TestMockData.getSqlContext();

        String jsonStr = "{'startDate':['2020-02-10'], 'endDate':['2020-02-21'], " +
                "'startAge':['0'], 'endAge':['60'], " +
                "'professionals':['professional2,professional4,professional6,professional8,professional10," +
                "professional11,professional13,professional15,professional17,professional19,professional22," +
                "professional24,professional27,professional28,professional33,professional35,professional37,professional44,professional46,professional49'], " +
                "'cities':['city43,city23,city11,city32,city15,city28,city56,city60,city14,city25,city16,city35,city18,city29,city34,city17,city48,city34'], " +
                "'sex':['male'], 'keywords':['苹果手机,笔记本电脑,电视,电子产品'], " +
                "categoryIds:['22,11,33,43,25,50,37,18,21,25,15,37,48,24,32,16']}";

        jsonObject = JSONObject.parseObject(jsonStr);
        //根据时间范围获取user_visit_action数据
        JavaRDD<Row> actionRDDByDateRangeRDD = UserVisitSessionAnalyzeSpark.getActionRDDByDateRange(sqlContext, jsonObject);
        printRDD("data/rdd/actionRDDByDateRangeRDD.txt", actionRDDByDateRangeRDD);
        //map分离出sessionId, 这里提取到属性，其它方法会用到
        sessionId2ActionRDD1 = UserVisitSessionAnalyzeSpark.getSessionId2ActionRDD1(actionRDDByDateRangeRDD);
        //聚合关联user_info
        JavaPairRDD<String, String> sessionId2AggregateInfoRDD = UserVisitSessionAnalyzeSpark.aggregateBySession(sqlContext, sessionId2ActionRDD1);
        printRDD("data/rdd/sessionId2AggregateInfoRDD.txt", sessionId2AggregateInfoRDD);

        return sessionId2AggregateInfoRDD;
    }

    public static void testFilterSessionAndAggregateStat(){
        JavaPairRDD<String, String> sessionId2AggregateInfoRDD = testAggregateBySession();
        SessionAggregateStatAccumulator accumulator = new SessionAggregateStatAccumulator();
        accumulator.reset();
        jsc.sc().register(accumulator, "SessionAggregateStatAccumulator");
        JavaPairRDD<String, String> filterSessionAndAggregateStatRDD = UserVisitSessionAnalyzeSpark.filterSessionAndAggregateStat(sessionId2AggregateInfoRDD, jsonObject, accumulator);
        printRDD("data/rdd/filterSessionAndAggregateStatRDD.txt", filterSessionAndAggregateStatRDD);

        System.out.println(accumulator.value());

    }


    public static void testRandomExtractSession(){
        JavaPairRDD<String, String> sessionId2aggregateInfoRDD = testAggregateBySession();
        UserVisitSessionAnalyzeSpark.randomExtractSession(jsc, 1L, sessionId2aggregateInfoRDD, sessionId2ActionRDD1);
    }

    @SuppressWarnings("rawtypes")
    public static void printRDD(String fileName, Object rdd){
        List collects = null;
        if (rdd instanceof JavaPairRDD){
            collects = ((JavaPairRDD) rdd).collect();
        }else {
            collects = ((JavaRDD)rdd).collect();
        }
        try {
            final BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(fileName));
            for (Object line : collects) {
                bufferedWriter.write(line.toString());
                bufferedWriter.newLine();
            }
            bufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        TestUserVisitSessionAnalyzeSpark.testFilterSessionAndAggregateStat();

//        TestUserVisitSessionAnalyzeSpark.testRandomExtractSession();

//        JavaPairRDD<String, String> rdd = TestUserVisitSessionAnalyzeSpark.testAggregateBySession();
//
//        outputRDD("Session2aggregateInfoRDD.txt", rdd);


    }
}
