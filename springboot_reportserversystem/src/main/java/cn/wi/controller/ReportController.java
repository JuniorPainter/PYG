package cn.wi.controller;

import cn.wi.pojo.Message;
import com.alibaba.fastjson.JSON;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;


/**
 * @author xianlawei
 * @ProjectName: Flink_PYG
 * @ClassName: ReportController
 * @Author: xianlawei
 * @Description: 用于接收Http请求数据
 * @date: 2019/9/4 21:39
 * RequestMapping 映射路径
 */
@RestController
@RequestMapping("report")
public class ReportController {

    /**
     * 注解注入  获取Bean对象
     */
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;
    //KafkaTemplate kafkaTemplate;

    /**
     * @param string              输入的内容
     * @param httpServletResponse 响应对象 封装了向客户端发送数据、发送响应头，发送响应状态码的方法
     */
    @RequestMapping(value = "put", method = RequestMethod.POST)
    public void putData(@RequestBody String string, HttpServletResponse httpServletResponse) {
        Message message = new Message();
        message.setCount(1);
        message.setMessage(string);
        message.setTimeStamp(System.currentTimeMillis());

        //将Bean对象转换成Json格式的字符串
        String jsonString = JSON.toJSONString(message);

        //获取kafka实例   发送数据到kafka
        //第一个参数topic
        kafkaTemplate.send("pygTopic", "test", jsonString);

        //发送的响应状态给前端用户/或者是访问接口
        PrintWriter printWriter = printWriter(httpServletResponse);
        printWriter.flush();
        printWriter.close();

        System.out.println("<<<:" + string);
    }

    public PrintWriter printWriter(HttpServletResponse httpServletResponse) {
        PrintWriter printWriter = null;
        try {
            //设置响应头 Json
            httpServletResponse.setContentType("application/json");
            //设置编码格式
            httpServletResponse.setCharacterEncoding("UTF-8");

            printWriter = new PrintWriter(httpServletResponse.getOutputStream());
            printWriter.write("Send Success");

        } catch (IOException e) {
            System.out.println("接收失败");
        }
        return printWriter;
    }
}
