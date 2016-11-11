package com.terri.redis;

import com.terri.redis.email.MailInfo;
import com.terri.redis.email.SendMail;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;

public class RemoveRedisKey {

    private static Jedis jedis;
    private static Jedis jedis_his;
    private static String redis_host = "";
    private static String redis_port = "";
    private static String redis_auth = "";
    private static String redis_his_host = "";
    private static String redis_his_port = "";
    private static String redis_his_auth = "";
    private static String timeout = "";
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    private static final String TODAY = sdf.format(Calendar.getInstance().getTime());
    private static final String[] HOURS_OF_DAY = new String[]{"00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23"};
    private static final List new_days_finished = new ArrayList<>();
    private static int total_finished_cnt = 0;
    private static String debug = "false";

    /**
     * 总离线天数
     */
    public static final int TOTAL_DAYS = 52;

    public static void main(String[] args) throws InterruptedException {
        //  loadConf("redis.properties");
        loadResources("redis.properties");
        jedis_his = new Jedis(redis_his_host, Integer.parseInt(redis_his_port), Integer.parseInt(timeout));
        jedis_his.auth(redis_his_auth);
        jedis = new Jedis(redis_host, Integer.parseInt(redis_port), Integer.parseInt(timeout));
        jedis.auth(redis_auth);
        jedis_his.select(2);
        RemoveAllKey("20160920", jedis_his);
        // loadHisUser();
        //  jedis.select(2);
        // cutNewUrlSet("201610923",jedis_his);
        //statisticsHash("20161024", "bus");
        //  RemoveAllKey();
        /*
        if (args.length == 2) {
            if (args[0].equals("del") && args[1].matches("[0-9]{8}")) {
                jedis.select(2);
                System.err.println("del day " + args[1]);
                Thread.sleep(1000 * 30);
                removeKeys(args[1]);
            } else if (args[0].equals("rem") && args[1].matches("[0-9]{8}")) {
                jedis.select(2);
                System.err.println("rem url " + args[1]);
                Thread.sleep(1000 * 30);
                removeUrl(args[1]);
            }
            return;
        }
        RemoveBeforekeys();
        cutUrlSet();
        handleOffLine();

        if ("00".equals(TODAY.substring(8, 10)) || new_days_finished.size() > 0) // 如果是０点启动则备份，或者有新离线任务有完成，则备份
        {
            jedis.bgsave();
        }
        System.out.println(Calendar.getInstance().getTime() + " finished");
         */
    }

    public static void loadHisUser() {
        Set<String> keys = jedis_his.keys("his:*");

        String lastL = "";
        String lastC = "";
        String lastProv = "";
        List<String> ka = new ArrayList<>();
        ka.addAll(keys);
        ka.sort((String k1, String k2) -> {
            if (k1.startsWith("his:bus")) {
                if (k2.startsWith("his:bus")) {
                    String[] a1 = k1.split(":");
                    String[] a2 = k2.split(":");
                    if (Integer.parseInt(a1[4]) != Integer.parseInt(a2[4])) {
                        return Integer.parseInt(a1[4]) - Integer.parseInt(a2[4]);
                    } else {
                        return Integer.parseInt(a1[5]) - Integer.parseInt(a2[5]);
                    }
                }

                return -1;
            } else {
                if (k2.startsWith("his:train")) {
                    String[] a1 = k1.split(":");
                    String[] a2 = k2.split(":");
                    if (Integer.parseInt(a1[5]) != Integer.parseInt(a2[5])) {
                        return Integer.parseInt(a1[5]) - Integer.parseInt(a2[5]);
                    } else if (Integer.parseInt(a1[6]) != Integer.parseInt(a2[6])) {
                        return Integer.parseInt(a1[6]) - Integer.parseInt(a2[6]);
                    } else {
                        return Integer.parseInt(a1[4]) - Integer.parseInt(a2[4]);
                    }
                }
                return 1;
            }

        });
        Set<String> city = new HashSet<>();
        Set<String> prov = new HashSet<>();
        String lastdt = "";

        Set<String> sl = new HashSet<>();
        Set<String> sc = new HashSet<>();
        Set<String> st = new HashSet<>();

        Map<String, Long> data = new HashMap<>();
        System.out.println(LocalTime.now());
        for (int i = 0; i < ka.size(); i++) {
            String[] a = ka.get(i).split(":");

            if (a.length < 6) {
                jedis_his.del(ka.get(i));
                continue;
            }

            if ("bus".equals(a[1])) {
                if (!"".equals(lastProv) && a[4].equals(lastProv)) {//新省份
                    String b[] = new String[city.size()];  //计算上一个省份
                    String pk = "his:bus:" + lastProv;
                    jedis_his.zunionstore(pk, city.toArray(b));
                    data.put("prov_" + lastProv, jedis_his.zcard(pk));
                    city.clear();
                    prov.add(pk);
                }
                city.add(ka.get(i));
                lastProv = a[4];
                data.put("city_" + a[4] + "_" + a[5], jedis_his.zcard(ka.get(i)));
                lastdt = "bus";
            }

            if ("train".equals(a[1])) {

                if (!lastdt.equals("train")) {//计算大巴最后一个省份
                    String b[] = new String[city.size()];
                    String pk = "his:bus:" + lastProv;
                    jedis_his.zunionstore(pk, city.toArray(b));
                    data.put("prov_" + a[4], jedis_his.zcard(pk));
                    city.clear();
                    prov.add(pk);
                    b = new String[prov.size()]; //大巴总计
                    pk = "his:bus";
                    jedis_his.zunionstore(pk, prov.toArray(b));
                    data.put("bus", jedis_his.zcard(pk));
                    prov.forEach(jedis_his::del);
                    jedis_his.del(pk);
                    prov.clear();
                }

                if (!"".equals(lastL) && !lastL.equals(a[5])) {//新路局

                    String[] b = new String[st.size()]; //计算旧车段
                    String pk = "his:train:che:" + lastL + ":" + lastC;
                    jedis_his.zunionstore(pk, st.toArray(b));
                    data.put("train_che_" + lastL + "_" + lastC, jedis_his.zcard(pk));
                    st.clear();
                    sc.add(pk);

                    b = new String[sc.size()]; //计算旧路局
                    pk = "his:train:Lu:" + lastL;
                    jedis_his.zunionstore(pk, sc.toArray(b));
                    data.put("train_lu_" + lastL, jedis_his.zcard(pk));
                    sc.forEach(jedis_his::del); //清理旧车次
                    sc.clear();
                    sl.add(pk);
                    System.out.println(LocalTime.now() + " 新路局 " + lastL);
                } else if (!"".equals(lastC) && !lastC.equals(a[6])) {//旧路局，新车段
                    String[] b = new String[st.size()];
                    String pk = "his:train:che:" + lastL + ":" + lastC;
                    jedis_his.zunionstore(pk, st.toArray(b));
                    data.put("train_che_" + lastL + "_" + lastC, jedis_his.zcard(pk));
                    st.clear();
                    sc.add(pk);
                    System.out.println(LocalTime.now() + " 新车段 " + lastC);
                }
                st.add(ka.get(i)); //保存当前车次信息
                data.put("train_line;" + a[4] + "_" + a[5] + "_" + a[6], jedis_his.zcard(ka.get(i)));
                lastC = a[6];
                lastL = a[5];
                lastdt = "train";
            }

        }

        String[] b = new String[st.size()];
        String pk = "his:train:che:" + lastL + ":" + lastC;
        jedis_his.zunionstore(pk, st.toArray(b));
        data.put("train_che_" + lastL + "_" + lastC, jedis_his.zcard(pk));
        st.clear();
        sc.add(pk);

        b = new String[sc.size()];
        pk = "his:train:Lu:" + lastL;
        jedis_his.zunionstore(pk, sc.toArray(b));
        data.put("train_lu_" + lastL, jedis_his.zcard(pk));
        sc.forEach(jedis_his::del);
        sc.clear();
        sl.add(pk);

        b = new String[sl.size()];
        pk = "his:train";
        jedis_his.zunionstore(pk, sl.toArray(b));
        data.put("train", jedis_his.zcard(pk));
        jedis_his.del(pk);
        sl.forEach(jedis_his::del);
        sl.clear();
        System.out.println(data);
        System.out.println(LocalTime.now());

    }

    public static void RemoveAllKey(String day_id, Jedis jedis) {
        Set<String> s = jedis.keys("wifilog:train:*:" + day_id + "*");
        int i = 0;
        Pipeline p = jedis.pipelined();
        for (String key : s) {
            p.del(key);

            if (++i % 10000 == 0) {
                p.sync();
                System.out.println(key);
            }
        }
        p.syncAndReturnAll();
        p = jedis.pipelined();
        Set<String> s2 = jedis.keys("wifilog:bus:*:" + day_id + "*");
        for (String key : s2) {
            p.del(key);

            if (++i % 10000 == 0) {
                p.sync();
                System.out.println(key);
            }
        }
        p.sync();
    }

    public static void statisticsHash(String day_id, String dt) {
        String key_1p = "wifilog:" + dt + ":v1:*:" + day_id;
        Set<String> t1 = jedis.keys(key_1p);
        Map<String, String> v1 = new HashMap<>();
        Map<String, String> v2 = new HashMap<>();
        int uv = 0;
        double sec = 0;
        t1.forEach(t -> {
            Map<String, String> tmp = jedis.hgetAll(t);
            for (Map.Entry<String, String> entry : tmp.entrySet()) {
                v1.merge(entry.getKey(), entry.getValue(), (oldv, nv) -> {
                    if (Integer.parseInt(oldv) > Integer.parseInt(nv)) {
                        return nv;
                    }
                    return oldv;
                });
            }
        });

        System.out.println("k1 size=" + t1.size());

        Set<String> t2 = jedis.keys(key_1p.replace(":v1", ":v2"));
        t2.forEach(t -> {
            Map<String, String> tmp = jedis.hgetAll(t);
            for (Map.Entry<String, String> entry : tmp.entrySet()) {
                v2.merge(entry.getKey(), entry.getValue(), (oldv, nv) -> {
                    if (Integer.parseInt(oldv) < Integer.parseInt(nv)) {
                        return nv;
                    }
                    return oldv;
                });
            }
        });
        for (Map.Entry<String, String> t : v1.entrySet()) {
            if (!t.getValue().equals(v2.get(t.getKey()))) {
                uv++;
                sec += Double.parseDouble(v2.get(t.getKey())) - Double.parseDouble(t.getValue());
            }
        }
        /*
           jedis.del("wifilog:" + dt + ":v2;"+day_id);
           jedis.del("wifilog:" + dt + ":v1;"+day_id);
           jedis.del("wifilog:" + dt + ":v;"+day_id);
         */

        System.out.println(dt + " 平均时长：" + sec / uv / 60 + "分钟 用户数：" + uv);

    }

    public static void statisticsNew(String day_id, String dt) {
        String key_1p = "wifilog:" + dt + ":v1:*:" + day_id;
        Set<String> t1 = jedis.keys(key_1p);
        int uv = 0;
        double sec = 0;
        String[] a = new String[t1.size()];
        System.out.println("k1 size=" + t1.size());
        jedis.zunionstore("wifilog:" + dt + ":v1;" + day_id, new ZParams().aggregate(ZParams.Aggregate.MIN), t1.toArray(a));
        Set<String> t2 = jedis.keys(key_1p.replace(":v1", ":v2"));
        a = new String[t2.size()];
        jedis.zunionstore("wifilog:" + dt + ":v2;" + day_id, new ZParams().aggregate(ZParams.Aggregate.MAX), t2.toArray(a));
        a = new String[t2.size()];
        jedis.zunionstore("wifilog:" + dt + ":v;" + day_id, new ZParams().weightsByDouble(new double[]{1, -1}), t2.toArray(a));
        Set<Tuple> s = jedis.zrangeWithScores("wifilog:" + dt + ":v;" + day_id, 0, -1);
        for (Tuple t : s) {
            if (t.getScore() > 0) {
                uv++;
                sec += t.getScore();
            }
        }
        jedis.del("wifilog:" + dt + ":v2;" + day_id);
        jedis.del("wifilog:" + dt + ":v1;" + day_id);
        jedis.del("wifilog:" + dt + ":v;" + day_id);

        System.out.println(dt + " 平均时长：" + sec / 60 + "分钟 用户数：" + uv);

    }

    public static void cutNewUrlSet(String day_id, Jedis jedis) {
        cutNewUrlSet(day_id, "train", jedis);
        cutNewUrlSet(day_id, "bus", jedis);
    }

    public static void cutNewUrlSet(String day_id, String dt, Jedis jedis) {
        if (TODAY.startsWith(day_id)) {
            throw new RuntimeException("非法的参数" + day_id + "，不允许裁剪当天的数据");
        }
        System.out.println("cut url set:" + day_id + " " + dt);
        int keep = 50;
        Set<String> keys = new HashSet<String>();
        GregorianCalendar gc = new GregorianCalendar();
        gc.add(GregorianCalendar.DAY_OF_YEAR, -3);
        String yesterday = sdf.format(gc.getTime());

        if (Integer.parseInt(day_id) > Integer.parseInt(yesterday.substring(0, 8))) {
            keep = 100;
        }

        String key1Pattern = "wifilog:" + dt + ":wlan:pv:url:*:" + day_id;
        Set<String> s = jedis.keys(key1Pattern);
        for (String tmp : s) {
            if (jedis.zcard(tmp) > keep) {
                jedis.zremrangeByRank(tmp, 0, -(keep + 1));
            }
            //System.out.println(jedis.zcard(tmp));
        }

        String key2Pattern = "wifilog:" + dt + ":wlan:uv:url:*:" + day_id + ":*";
        Set<String> s2 = jedis.keys(key2Pattern);
        System.out.println(s2.size());
        Set<String> key2del = new HashSet<>();
        for (String tmp : s2) {
            String tmp2 = tmp.replace(":uv", ":pv").substring(0, tmp.lastIndexOf(":"));
            // System.out.println(tmp2);
            if (jedis.zrank(tmp2.substring(0, tmp.lastIndexOf(":")), tmp.substring(tmp.lastIndexOf(":") + 1)) == null) {
                //System.out.println(tmp);
                key2del.add(tmp);
                //jedis.del(tmp);
            }

        }
        Pipeline p = jedis.pipelined();
        int m = 0;
        for (String key : key2del) {
            p.del(key);
            if (++m % 10000 == 0) {
                System.out.println(key);
                p.sync();
            }
        }
        System.out.println(day_id + " " + dt + " over");
        p.sync();

    }

    public static void loadResources(String path) {
        Properties prop = new Properties();
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        try (InputStream is = classloader.getResourceAsStream(path)) {
            prop.load(is);
            initCfg(prop);
        } catch (IOException ex) {
            Logger.getLogger(RemoveRedisKey.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    public static void loadConf(String path) {
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(path);) {
            props.load(in);
            initCfg(props);
        } catch (IOException ex) {
            Logger.getLogger(RemoveRedisKey.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private static void initCfg(Properties props) {
        redis_host = props.getProperty("redis.host").trim();
        redis_port = props.getProperty("redis.port").trim();
        redis_auth = props.getProperty("redis.auth").trim();
        timeout = props.getProperty("redis.timeout").trim();
        //   debug = props.get("debug").toString();
        if (redis_host == null || redis_host.trim().length() == 0) {
            throw new RuntimeException("redis.properties not found or illagel parameters.");
        }

    }

    private static boolean isNewFinished(String val) {
        String str;
        str = val;
        // 只允数字  
        String regEx = "[^0-9]";
        Pattern p = Pattern.compile(regEx);
        Matcher m = p.matcher(val);
        //替换与模式匹配的所有字符（即非数字的字符将被""替换）
        str = m.replaceAll("").trim();
        // System.out.println("after replace "+str);
        if (str.length() == 14) {
            long from = 0;
            try {
                from = sdf.parse(str).getTime();
            } catch (ParseException ex) {
                Logger.getLogger(RemoveRedisKey.class.getName()).log(Level.SEVERE, null, ex);
            }

            if (from == 0) {
                return false;
            }

            long to = Calendar.getInstance().getTime().getTime();
            int hours = (int) ((to - from) / (1000 * 60 * 60));
            return hours == 1;
        }
        return false;
    }

    private static MailInfo getEmailMsg() {
        String msg = "";
        String title = "";
        if (new_days_finished.size() > 0) {

            if (total_finished_cnt == TOTAL_DAYS) {
                title = "恭喜：全部完成";
            } else {
                title = "通知：";
            }

            msg += new_days_finished.toString() + " 完成,共完成" + total_finished_cnt + "/" + TOTAL_DAYS + " 天";
        }

        double rest_memory = OSHelper.getFreePhysicalMemorySize();
        if (rest_memory < 10) {//剩余内存小于10g 告警
            title = "警告：";
            msg += System.lineSeparator() + "系统可用内存不足，" + rest_memory + "g 请整理";
        }
        if (!title.isEmpty()) {
            msg += System.lineSeparator() + TODAY;
        } else {
            return null;
        }

        MailInfo m = new MailInfo();
        m.setSubject(title);
        m.setContent(msg);
        return m;
    }

    public static void handleOffLine() {
        jedis.disconnect();
        jedis.connect();
        jedis.select(2);
        String keyPattern = "off-line:fininshed:*";
        Set<String> keys = jedis.keys(keyPattern);

        if (keys.isEmpty()) {
            return;
        }

        List<String> days;
        days = new ArrayList<>();
        keys.stream().forEach((String key) -> {
            String str = jedis.get(key);
            String day = key.substring(key.length() - 8);//获取日期 day_id,并进行清理
            //   cutUrlSet(day);
            days.add(day);
            if (isNewFinished(str)) {
                new_days_finished.add(day);
            }
        });
        if (days.isEmpty()) {
            return;
        }
        if (new_days_finished.size() > 0) { //有新任务完成才再次统计
            Collections.sort(days);
            total_finished_cnt = days.size();
            for (int i = 0; i < total_finished_cnt; i++) {
                //statistics(days.get(i));
            }
        }

        MailInfo m = getEmailMsg();
        if (m != null) {
            SendMail.send(m);
        }
    }

}
