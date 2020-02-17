package utils;

/**
 * 为了单独处理由JSON处理后的字符串参数列表，对参数进行比对
 * 之所以加validUtils，是为了有效的性能优化
 */
public class ValidUtils {
    /**
     *  主要用于整型数字比较，是否在指定范围内[start, end]
     */
    public static boolean between(
            String data, String field,
            String params, String startParamField, String endParamField){

        String startParam = StringUtils.getFieldFromConcatString(params, "\\|", startParamField);
        String endParam = StringUtils.getFieldFromConcatString(params, "\\|", endParamField);
        if (startParam == null || endParam == null) return true;

        int startValue = Integer.parseInt(startParam);
        int endValue = Integer.parseInt(endParam);

        String fieldValue = StringUtils.getFieldFromConcatString(data, "\\|", field);
        if (fieldValue != null){
            int value = Integer.parseInt(fieldValue);
            return value >= startValue && value <= endValue;
        }
        return false;
    }

    /**
     *  数据中只要存在一个字段值与参数列表的其中一个字段值相同即返回true
     */
    public static boolean in(String data, String dataField, String params, String paramField){

        String paramFieldValue = StringUtils.getFieldFromConcatString(params, "\\|", paramField);
        if (paramFieldValue == null){
            return true;
        }

        String[] paramValues = paramFieldValue.split(",");

        String dataValue = StringUtils.getFieldFromConcatString(data, "\\|", dataField);

        if (dataValue != null){
            String[] fieldValues = dataValue.split(",");
            //考虑searchKeyword的多对多的匹配
            for (String value : fieldValues) {
                for (String paramValue : paramValues) {
                    if (value.equals(paramValue)) return true;
                }
            }
        }
        return false;
    }

    /**
     * 对比两个字段的value是否相同
     */
    public static boolean equal(String data, String field, String params, String paramField){
        String paramValue = StringUtils.getFieldFromConcatString(params, "\\|", paramField);

        if (paramValue == null){
            return true;
        }

        String value = StringUtils.getFieldFromConcatString(
                data, "\\|", field);

        if (value != null){
            return value.equals(paramValue);
        }

        return false;
    }
}
