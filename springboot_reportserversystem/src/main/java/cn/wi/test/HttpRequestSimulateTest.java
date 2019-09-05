package cn.wi.test;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;


/**
 * @ProjectName: Flink_PYG
 * @ClassName: HttpTest
 * @Author: xianlawei
 * @Description: 模拟浏览器发送Http请求
 * @date: 2019/9/4 22:13
 */
public class HttpRequestSimulateTest {
    public static void main(String[] args) throws IOException {
        String address = "http://localhost:8090/report/put";
        String testInfo = "=====test====";
        sendData(address, testInfo);

    }

    /**
     * 模拟url发送请求
     */
    public static void sendData(String address, String testInfo) throws IOException {
        String string = "";
        //新建URL连接对象
        URL url = new URL(address);

        //获取连接
        HttpURLConnection httpUrlConnection = (HttpURLConnection) url.openConnection();

        //设置URL属性
        //连接超时
        httpUrlConnection.setConnectTimeout(60000);
        //设置用户缓存
        httpUrlConnection.setUseCaches(false);
        //设置请求方式
        httpUrlConnection.setRequestMethod("POST");
        //设置URL连接可用于输入和/或输出
        httpUrlConnection.setDoInput(true);
        httpUrlConnection.setDoOutput(true);
        //请求格式类型
        httpUrlConnection.setRequestProperty("Content-Type", "application/json");
        //模拟的用户代理
        httpUrlConnection.setRequestProperty(
                "User-Agent",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " +
                        "AppleWebKit/537.36 (KHTML, like Gecko) " +
                        "Chrome/76.0.3809.132 Safari/537.36");

        //发送数据
        OutputStream outputStream = httpUrlConnection.getOutputStream();
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(outputStream);
        bufferedOutputStream.write(testInfo.getBytes());
        bufferedOutputStream.flush();
        bufferedOutputStream.close();

        //接收响应状态
        InputStream inputStream = httpUrlConnection.getInputStream();
        final int length = 1024;
        byte[] bytes = new byte[length];
        while (inputStream.read(bytes, 0, length) != -1) {
            string = new String(bytes);
        }
        System.out.println("<<<:" + string);
    }
}
