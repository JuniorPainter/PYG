package cn.wi.pojo;

/**
 * @ProjectName: Flink_PYG
 * @ClassName: UserBrowse
 * @Author: xianlawei
 * @Description: 点击流日志实体类
 * @date: 2019/9/5 15:13
 */
public class UserBrowse {

    /**
     * 频道ID
     */
    private long channelId;

    /**
     * 产品的类别ID
     */
    private long categoryId;

    /**
     * 产品ID
     */
    private long produceId;

    /**
     * 国家
     */
    private String country;

    /**
     * 省份
     */
    private String province;

    /**
     * 城市
     */
    private String city;
    /**
     * 网络方式
     */
    private String network;
    /**
     * 来源方式
     */
    private String source;

    /**
     * 浏览器类型
     */
    private String browserType;

    /**
     * 进入网站时间
     */

    private Long entryTime;

    /**
     * 离开网站时间
     */
    private long leaveTime;

    /**
     * 用户的ID
     */
    private long userId;

    /**
     * 日志产生时间
     */
    private long timestamp;

    public long getChannelId() {
        return channelId;
    }

    public void setChannelId(long channelId) {
        this.channelId = channelId;
    }

    public long getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(long categoryId) {
        this.categoryId = categoryId;
    }

    public long getProduceId() {
        return produceId;
    }

    public void setProduceId(long produceId) {
        this.produceId = produceId;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getNetwork() {
        return network;
    }

    public void setNetwork(String network) {
        this.network = network;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getBrowserType() {
        return browserType;
    }

    public void setBrowserType(String browserType) {
        this.browserType = browserType;
    }

    public Long getEntryTime() {
        return entryTime;
    }

    public void setEntryTime(Long entryTime) {
        this.entryTime = entryTime;
    }

    public long getLeaveTime() {
        return leaveTime;
    }

    public void setLeaveTime(long leaveTime) {
        this.leaveTime = leaveTime;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "UserBrowse{" +
                "channelId=" + channelId +
                ", categoryId=" + categoryId +
                ", produceId=" + produceId +
                ", country='" + country + '\'' +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                ", network='" + network + '\'' +
                ", source='" + source + '\'' +
                ", browserType='" + browserType + '\'' +
                ", entryTime=" + entryTime +
                ", leaveTime=" + leaveTime +
                ", userId=" + userId +
                ", timestamp=" + timestamp +
                '}';
    }
}
