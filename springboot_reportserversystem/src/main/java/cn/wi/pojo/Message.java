package cn.wi.pojo;

/**
 * @ProjectName: Flink_PYG
 * @ClassName: Message
 * @Author: xianlawei
 * @Description: bean对象
 * @date: 2019/9/4 21:50
 */
public class Message {
    /**
     * 发送条数
     * 发送的消息
     * 发送的时间
     */
    private int count;
    private String message;
    private Long timeStamp;

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "Message{" +
                "count=" + count +
                ", message='" + message + '\'' +
                ", timeStamp=" + timeStamp +
                '}';
    }
}
