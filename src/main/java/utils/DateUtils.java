package utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtils {
    //保证线程安全
    private static final ThreadLocal<SimpleDateFormat> localDateFormat = new ThreadLocal<>();
    //时间格式
    private static final String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    //日期格式
    private static final String DATE_FORMAT = "yyyy-MM-dd";
    //日期key格式
    private static final String DATE_KEY_FORMAT = "yyyyMMdd";

    /**
     *  判断time1在time2之前
     */
    public static boolean before(String time1, String time2){
        SimpleDateFormat sdf = getTimeFormat();
        try {
            Date dateTime1 = sdf.parse(time1);
            Date dateTime2 = sdf.parse(time2);
            if (dateTime1.before(dateTime2)) return true;
        } catch (ParseException e) {
            e.printStackTrace();
            System.out.println("DateUtils Error == before() == 时间格式错误");
        }
        return false;
    }
    /**
     *  判断time1在time2之后
     */
    public static boolean after(String time1, String time2) {
        SimpleDateFormat sdf = getTimeFormat();
        try {
            Date dateTime1 = sdf.parse(time1);
            Date dateTime2 = sdf.parse(time2);

            if(dateTime1.after(dateTime2)) return true;
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("DateUtils Error == after() == 时间格式错误");
        }
        return false;
    }

    /**
     * 计算时间差
     */
    public static int minus(String time1, String time2) {
        SimpleDateFormat sdf = getTimeFormat();
        try {
            Date datetime1 = sdf.parse(time1);
            Date datetime2 = sdf.parse(time2);

            long millisecond = datetime1.getTime() - datetime2.getTime();

            return Integer.parseInt(String.valueOf(millisecond / 1000));
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("DateUtils Error == minus() == 时间格式错误");
        }
        return 0;
    }

    /**
     * 获取今天的日期
     */
    public static String getTodayDate() {
        SimpleDateFormat sdf = getDateFormat();
        return sdf.format(new Date());
    }

    /**
     * 获取昨天的日期
     */
    public static String getYesterdayDate() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.DAY_OF_YEAR, -1);

        Date date = cal.getTime();

        SimpleDateFormat sdf = getDateFormat();
        return sdf.format(date);
    }

    /**
     * 获取精确到小时的时间
     */
    public static String getDateHour(String dateTime){
        String date = dateTime.split(" ")[0];
        String time = dateTime.split(" ")[1];
        String hour = time.split(":")[0];
        return date + "_" + hour;
    }

    /**
     * 将时间转为日期String
     */
    public static String formatDate(Date date) {
        SimpleDateFormat sdf = getDateFormat();
        return sdf.format(date);
    }

    /**
     * 将时间转为时间String
     */
    public static String formatTime(Date date) {
        SimpleDateFormat sdf = getTimeFormat();
        return sdf.format(date);
    }

    /**
     * 将日期转为日期key String
     */
    public static String formatDateKey(Date date) {
        SimpleDateFormat sdf = getDateKeyFormat();
        return sdf.format(date);
    }

    /**
     * 将日期key String转为时间类
     */
    public static Date parseDateKey(String datekey) {
        SimpleDateFormat sdf = getDateKeyFormat();
        try {
            return sdf.parse(datekey);
        } catch (ParseException e) {
            e.printStackTrace();
            System.out.println("DateUtils Error == parseDateKey() == 时间格式错误");
        }
        return null;
    }

    /**
     * 将String转为时间类
     */
    public static Date parseTime(String time) {
        SimpleDateFormat sdf = getTimeFormat();
        try {
            return sdf.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
            System.out.println("DateUtils Error == parseTIme() == 时间格式错误");
        }
        return null;
    }

    private static SimpleDateFormat getTimeFormat(){
        SimpleDateFormat sdf = localDateFormat.get();
        if (sdf == null){
            sdf = new SimpleDateFormat(TIME_FORMAT);
        }
        return sdf;
    }

    private static SimpleDateFormat getDateFormat(){
        SimpleDateFormat sdf = localDateFormat.get();
        if (sdf == null){
            sdf = new SimpleDateFormat(DATE_FORMAT);
        }
        return sdf;
    }

    private static SimpleDateFormat getDateKeyFormat(){
        SimpleDateFormat sdf = localDateFormat.get();
        if (sdf == null){
            sdf = new SimpleDateFormat(DATE_KEY_FORMAT);
        }
        return sdf;
    }

}
