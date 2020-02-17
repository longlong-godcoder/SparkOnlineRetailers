package spark.session;

import constant.Constants;
import org.apache.spark.util.AccumulatorV2;
import utils.StringUtils;

/**
 * 统计所有session的访问时长与步长，在各个区间范围的数量
 */
public class SeesionAggrStatAccumulator extends AccumulatorV2<String, String> {

    private String accumulator;

    /**
     *  当AccumulatorV2中存在类似数据不存在这种问题时，是否结束程序
     */
    @Override
    public boolean isZero() {
        System.out.println("==========存在异常数据使数据累加失败===============");
        return true;

    }

    @Override
    public AccumulatorV2<String, String> copy() {
        SeesionAggrStatAccumulator newAccumulator = new SeesionAggrStatAccumulator();
        newAccumulator.accumulator = this.accumulator;
        return newAccumulator;
    }

    @Override
    public void reset() {
        accumulator =  Constants.SESSION_COUNT + "=0|"
                + Constants.TIME_PERIOD_1s_3s + "=0|"
                + Constants.TIME_PERIOD_4s_6s + "=0|"
                + Constants.TIME_PERIOD_7s_9s + "=0|"
                + Constants.TIME_PERIOD_10s_30s + "=0|"
                + Constants.TIME_PERIOD_30s_60s + "=0|"
                + Constants.TIME_PERIOD_1m_3m + "=0|"
                + Constants.TIME_PERIOD_3m_10m + "=0|"
                + Constants.TIME_PERIOD_10m_30m + "=0|"
                + Constants.TIME_PERIOD_30m + "=0|"
                + Constants.STEP_PERIOD_1_3 + "=0|"
                + Constants.STEP_PERIOD_4_6 + "=0|"
                + Constants.STEP_PERIOD_7_9 + "=0|"
                + Constants.STEP_PERIOD_10_30 + "=0|"
                + Constants.STEP_PERIOD_30_60 + "=0|"
                + Constants.STEP_PERIOD_60 + "=0";
    }

    /**
     * 数据累加
     */
    @Override
    public void add(String field) {
        if (StringUtils.isNotEmpty(field)){
            String oldValue = StringUtils.getFieldFromConcatString(accumulator, "\\|", field);
            if (oldValue != null){
                int newValue = Integer.parseInt(oldValue) + 1;
                StringUtils.setFieldInConcatString(accumulator, "\\|", field, String.valueOf(newValue));
            }
        }
    }

    /**
     * 合并数据
     */
    @Override
    public void merge(AccumulatorV2<String, String> accumulatorV2) {
        String accumulator2 = accumulatorV2.value();
        String[] fieldAndValues = accumulator2.split("\\|");
        for (String fieldAndValue : fieldAndValues) {
            String field = fieldAndValue.split("=")[0];
            String value = fieldAndValue.split("=")[1];
            String oldValue = StringUtils.getFieldFromConcatString(accumulator, "\\|", field);
            int newValue = Integer.parseInt(value) + Integer.parseInt(oldValue);
            StringUtils.setFieldInConcatString(accumulator, "\\|", field, String.valueOf(newValue));
        }
    }

    /**
     * 对外访问数据结果
     */
    @Override
    public String value() {
        return accumulator;
    }

}
