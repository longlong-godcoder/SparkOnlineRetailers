package testSessionAnalysis;

import com.alibaba.fastjson.JSONObject;
import constant.Constants;
import spark.session.SessionAggregateStatAccumulator;
import testDAO.TestValidUtils;
import utils.ParamUtils;
import utils.StringUtils;
import utils.ValidUtils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class TestAccumulator {

    private static SessionAggregateStatAccumulator sessionAggregateStatAccumulator;

    public static String getParams(JSONObject taskParam){
        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);
        //拼接所有字段，如果为null默认空串
        String _params = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
                + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
                + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
                + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
                + (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
                + (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds: "");

        System.out.println(_params);
        return _params;
    }

    public static void test(){

        String jsonStr = "{'startDate':['2020-02-10'], 'endDate':['2020-02-21'], " +
                "'startAge':['0'], 'endAge':['60'], " +
                "'professionals':['professional2,professional4,professional6,professional8,professional10," +
                "professional11,professional13,professional15,professional17,professional19,professional22," +
                "professional24,professional27,professional28,professional33,professional35,professional37,professional44,professional46,professional49'], " +
                "'cities':['city43,city23,city11,city32,city15,city28,city56,city60,city14,city25,city16,city35,city18,city29,city34,city17,city48,city34'], " +
                "'sex':['male'], 'keywords':['苹果手机,笔记本电脑,电视,电子产品'], " +
                "categoryIds:['22,11,33,43,25,50,37,18,21,25,15,37,48,24,32,16']}";

        JSONObject jsonObject = JSONObject.parseObject(jsonStr);
        BufferedReader bufferedReader = null;
        try {
            bufferedReader = new BufferedReader(new FileReader("data/rdd/sessionId2AggregateInfoRDD.txt"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        String params = getParams(jsonObject);
        ArrayList<String> fitList = new ArrayList<>();
        sessionAggregateStatAccumulator = new SessionAggregateStatAccumulator();
        sessionAggregateStatAccumulator.reset();
        int[] unfitCount = {0, 0, 0, 0, 0, 0};
        int count = 0;
        String line;
        try {
            assert bufferedReader != null;
            while ((line = bufferedReader.readLine()) != null){
                count ++;
                int length = line.substring(1, line.length() - 1).split(",")[0].length();
                String aggregateInfo = line.substring(length + 2, line.length() - 1);
                if (!ValidUtils.between(aggregateInfo,
                        Constants.FIELD_AGE, params, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                    unfitCount[0]++;
                    continue;
                }
                //职业范围
                if (!ValidUtils.in(aggregateInfo,
                        Constants.FIELD_PROFESSIONAL, params, Constants.PARAM_PROFESSIONALS)) {
                    unfitCount[1]++;
                    continue;
                }
                //城市范围
                if (!ValidUtils.in(aggregateInfo,
                        Constants.FIELD_CITY, params, Constants.PARAM_CITIES)) {
                    unfitCount[2]++;
                    continue;
                }
                //性别
                if (!ValidUtils.equal(aggregateInfo,
                        Constants.FIELD_SEX, params, Constants.PARAM_SEX)) {
                    unfitCount[3]++;
                    continue;
                }
                //搜索关键字匹配，只要包含一个即可
                if (!ValidUtils.in(aggregateInfo,
                        Constants.FIELD_SEARCH_KEYWORDS, params, Constants.PARAM_KEYWORDS)) {
                    unfitCount[4]++;
                    continue;
                }
                //点击品类id
                if (!ValidUtils.in(aggregateInfo,
                        Constants.FIELD_CLICK_CATEGORY_IDS, params, Constants.PARAM_CATEGORY_IDS)) {
                    unfitCount[5]++;
                    continue;
                }

                sessionAggregateStatAccumulator.add(Constants.SESSION_COUNT);

                long visitLength = Long.parseLong(StringUtils.getFieldFromConcatString(aggregateInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
                long stepLength = Long.parseLong(StringUtils.getFieldFromConcatString(aggregateInfo, "\\|", Constants.FIELD_STEP_LENGTH));

                calculateVisitLength(visitLength);
                calculateStepLength(stepLength);

                System.out.println("====找到一条符合过滤条件的数据====");
                fitList.add(aggregateInfo);
            }
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


        int sum = 0;
        for (int i = 0; i < unfitCount.length; i++) {
            sum += unfitCount[i];
        }

        for (int i = 0; i < unfitCount.length; i++) {
            System.out.print(unfitCount[i] + " ");
        }
        System.out.println("样本总数：" + count);
        System.out.println("不匹配总数： " + sum);
        fitList.forEach(System.out::println);
        System.out.println(sessionAggregateStatAccumulator.value());
    }

    private static void calculateVisitLength(long visitLength) {
        if(visitLength >=1 && visitLength <= 3) {
            sessionAggregateStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
        } else if(visitLength >=4 && visitLength <= 6) {
            sessionAggregateStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
        } else if(visitLength >=7 && visitLength <= 9) {
            sessionAggregateStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
        } else if(visitLength >=10 && visitLength <= 30) {
            sessionAggregateStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
        } else if(visitLength > 30 && visitLength <= 60) {
            sessionAggregateStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
        } else if(visitLength > 60 && visitLength <= 180) {
            sessionAggregateStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
        } else if(visitLength > 180 && visitLength <= 600) {
            sessionAggregateStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
        } else if(visitLength > 600 && visitLength <= 1800) {
            sessionAggregateStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
        } else if(visitLength > 1800) {
            sessionAggregateStatAccumulator.add(Constants.TIME_PERIOD_30m);
        }
    }
    //计算访问步长在什么区间，驱动累加器
    private static void calculateStepLength(long stepLength) {
        if(stepLength >= 1 && stepLength <= 3) {
            sessionAggregateStatAccumulator.add(Constants.STEP_PERIOD_1_3);
        } else if(stepLength >= 4 && stepLength <= 6) {
            sessionAggregateStatAccumulator.add(Constants.STEP_PERIOD_4_6);
        } else if(stepLength >= 7 && stepLength <= 9) {
            sessionAggregateStatAccumulator.add(Constants.STEP_PERIOD_7_9);
        } else if(stepLength >= 10 && stepLength <= 30) {
            sessionAggregateStatAccumulator.add(Constants.STEP_PERIOD_10_30);
        } else if(stepLength > 30 && stepLength <= 60) {
            sessionAggregateStatAccumulator.add(Constants.STEP_PERIOD_30_60);
        } else if(stepLength > 60) {
            sessionAggregateStatAccumulator.add(Constants.STEP_PERIOD_60);
        }
    }

    public static void main(String[] args) {
        TestAccumulator.test();
    }
}
