package cn.wi.test;

import cn.wi.pojo.UserBrowse;
import com.alibaba.fastjson.JSONObject;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * @ProjectName: Flink_PYG
 * @ClassName: UserBrowseRecord
 * @Author: xianlawei
 * @Description: 模拟生产点击流日志消息
 *               模拟用户浏览网站日志
 * @date: 2019/9/5 15:11
 */

public class UserBrowseRecord {

    /**
     * 频道id集合
     * 产品类别id集合
     * 产品id集合
     * 用户id集合
     */

    private static Long[] channelID = new Long[]{1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L};
    private static Long[] categoryID = new Long[]{1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L};
    private static Long[] commodityID = new Long[]{1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L};
    private static Long[] userID = new Long[]{1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L};

    /**
     * 地区
     * 地区-国家集合
     */
    private static String[] countries = new String[]{"America", "china"};
    /**
     * 地区-省集合
     */
    private static String[] provinces = new String[]{"America", "china"};
    /**
     * 地区-市集合
     */
    private static String[] cities = new String[]{"America", "china"};

    /**
     * 网络方式
     */
    private static String[] networks = new String[]{"电信", "移动", "联通"};

    /**
     * 来源方式
     */
    private static String[] sources = new String[]{"直接输入", "百度跳转", "360搜索跳转", "必应跳转"};

    /**
     * 浏览器
     */
    private static String[] Browser = new String[]{"火狐", "qq浏览器", "360浏览器", "谷歌浏览器"};

    /**
     * 打开时间 离开时间
     */
    private static List<Long[]> useTimeLog = produceTimes();

    /**
     * 获取时间
     */
    final static int NUMBER = 100;

    public static List<Long[]> produceTimes() {
        List<Long[]> useTimeLog = new ArrayList<Long[]>();
        for (int i = 0; i < NUMBER; i++) {
            Long[] timesArray = getTimes("2018-12-12 24:60:60:000");
            useTimeLog.add(timesArray);
        }
        return useTimeLog;
    }


    private static Long[] getTimes(String time) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS");
        try {
            Date date = dateFormat.parse(time);

            long timeTemp = date.getTime();
            Random random = new Random();
            int randomInt = random.nextInt(10);

            long startTime = timeTemp - randomInt * 3600 * 1000;
            long endTime = startTime + randomInt * 3600 * 1000;

            return new Long[]{startTime, endTime};
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return new Long[]{0L, 0L};
    }

    public static void main(String[] args) throws IOException {
        Random random = new Random();
        for (int i = 0; i < 6; i++) {
            //频道id 类别id 产品id 用户id 打开时间 离开时间 地区 网络方式 来源方式 浏览器
            UserBrowse userBrowse = new UserBrowse();
            userBrowse.setChannelId(channelID[random.nextInt(channelID.length)]);
            userBrowse.setCategoryId(categoryID[random.nextInt(categoryID.length)]);
            userBrowse.setProduceId(commodityID[random.nextInt(commodityID.length)]);
            userBrowse.setUserId(userID[random.nextInt(userID.length)]);
            userBrowse.setCountry(countries[random.nextInt(countries.length)]);
            userBrowse.setProvince(provinces[random.nextInt(provinces.length)]);
            userBrowse.setCity(cities[random.nextInt(cities.length)]);
            userBrowse.setNetwork(networks[random.nextInt(networks.length)]);
            userBrowse.setSource(sources[random.nextInt(sources.length)]);
            userBrowse.setBrowserType(Browser[random.nextInt(Browser.length)]);

            //new Date().getTime
            userBrowse.setTimestamp(System.currentTimeMillis());

            Long[] times = useTimeLog.get(random.nextInt(useTimeLog.size()));
            Long tme = times[0];
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS");
            Date date = new Date(tme);
            String format = dateFormat.format(date);
            System.out.println("<<<<<<<<<<<:" + format);

            userBrowse.setEntryTime(times[0]);
            userBrowse.setLeaveTime(times[1]);

            String jsonString = JSONObject.toJSONString(userBrowse);
            System.out.println(jsonString);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            HttpRequestSimulateTest.sendData("http://localhost:8090/report/put", jsonString);
        }
    }
}
