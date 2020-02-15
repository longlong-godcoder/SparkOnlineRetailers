package conf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 获取参数值类
 */
public class ConfigurationManager {

    private static Properties properties = new Properties();

    static {

        InputStream inputStream = ConfigurationManager.class.getClassLoader().getResourceAsStream("my.properties");

        try {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("my.properties配置文件加载失败");
        }
    }

    /**
     * 获取String参数
     */
    public static String getProperty(String key){
        return properties.getProperty(key);
    }

    /**
     *  获取Integer参数
     */
    public static Integer getInteger(String key){

        try {
            return Integer.parseInt(properties.getProperty(key));
        } catch (NumberFormatException e) {
            e.printStackTrace();
            System.out.println("获取Integer类型参数失败");
        }
        return 0;
    }

    /**
     *  获取Boolean参数
     */
    public static Boolean getBoolean(String key){
        try {
            return Boolean.parseBoolean(properties.getProperty(key));
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("获取Boolean类型参数失败");
        }
        return false;
    }

    public static Long getLong(String key){
        try {
            return Long.parseLong(properties.getProperty(key));
        } catch (NumberFormatException e) {
            e.printStackTrace();
            System.out.println("获取Long类型参数失败");
        }
        return 0L;
    }
}
