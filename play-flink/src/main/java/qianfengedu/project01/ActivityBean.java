package qianfengedu.project01;

/**
 * @ClassName: ActivityBean
 * @Description: 日志字段Bean
 * @Author: Jokey Zhou
 * @Date: 2020/5/17
 * @赛博世界并不是辽阔的荒野，数据也不全是冰冷的记录，它是亲人的笑靥，它是我们的记忆。
 */
public class ActivityBean {

    public String uid;

    public String aid;

    public String activityName;

    public String time;

    public Integer eventType;

    public String province;

    public ActivityBean() {
    }

    public ActivityBean(String uid, String aid, String activityName,
                        String time, Integer eventType, String province) {
        this.uid = uid;
        this.aid = aid;
        this.activityName = activityName;
        this.time = time;
        this.eventType = eventType;
        this.province = province;
    }

    @Override
    public String toString() {
        return "ActivityBean{" +
                "uid='" + uid + '\'' +
                ", aid='" + aid + '\'' +
                ", activityName='" + activityName + '\'' +
                ", time='" + time + '\'' +
                ", eventType=" + eventType +
                ", province='" + province + '\'' +
                '}';
    }

    public static ActivityBean of(String uid, String aid, String activityName,
                                  String time, Integer eventType, String province) {
        return new ActivityBean(uid, aid, activityName, time, eventType, province);
    }

}
