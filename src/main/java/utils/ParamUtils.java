package utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class ParamUtils {
    /**
     * 从命令行参数中提取任务id
     */
    public static Long getTaskIdFromArgs(String[] args, String taskType){
        return null;
    }

    /**
     * 从JSON对象中提取参数
     */
    public static String getParam(JSONObject jsonObject, String field){
        JSONArray jsonArray = jsonObject.getJSONArray(field);
        if (jsonArray != null && jsonArray.size() > 0){
            return jsonArray.getString(0);
        }
        System.out.println("ParamUtils == getParam() 失败");
        return null;
    }

}
