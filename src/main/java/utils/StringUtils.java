package utils;

public class StringUtils {

    /**
     * 判断空串或者空
     */
    public static boolean isEmpty(String str){
        return str == null || "".equals(str);
    }

    /**
     * 判断是否不为空，其实就是isEmpty求反
     */
    public static boolean isNotEmpty(String str){
        return !isEmpty(str);
    }

    /**
     * 去掉字符串两侧逗号
     */
    public static String trimComma(String str){
        if (str.startsWith(",")){
            str = str.substring(1);
        }
        if (str.endsWith(",")){
            str = str.substring(0, str.length() - 1);
        }
        return str;
    }

    /**
     * 补全两位数字
     */
    public static String fullfill(String str){
        if (str.length() == 2){
            return str;
        }else {
            return "0" + str;
        }
    }

    /**
     *  从拼接的字符串中提取字段
     */
    public static String getFieldFromConcatString(String str, String delimiter, String field){

        String[] fields = str.split(delimiter);
        for (String singleField : fields) {
            // 存在value为空的情况， searchKeywords=|clickCategoryIds=1,2,3
            if (singleField.split("=").length == 2){
                String fieldName = singleField.split("=")[0];
                String fieldValue = singleField.split("=")[1];
                if (fieldName.equals(field)){
                    return fieldValue;
                }
            }
        }

        return null;
    }

    /**
     *  从拼接的字符串中给字段设置值
     */
    public static String setFieldInConcatString(String str, String delimiter, String field, String value){
        String[] fields = str.split(delimiter);
        //查找到field并修改value的值
        for (int i = 0; i < fields.length; i++) {
            String fieldName = fields[i].split("=")[0];
            if(fieldName.equals(field)) {
                fields[i] = fieldName + value;
                break;
            }
        }

        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            buffer.append(fields[i]);
            if (i < fields.length - 1){
                buffer.append("|");
            }
        }
        return buffer.toString();
    }


}
