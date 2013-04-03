package com.hoccer.talk.server;

public class TalkServerConfiguration {

    /** True to enable GCM */
    public static final boolean GCM_ENABLE = false;

    /** GCM API key */
    public static final String GCM_API_KEY = "AIzaSyA25wabV4kSQTaF73LTgTkjmw0yZ8inVr8";

    /** GCM wake message time-to-live (seconds) */
    public static final int GCM_WAKE_TTL =  1 * 7 * 24 * 3600; /* 1 week */

    /** True to enable APNS */
    public static final boolean  APNS_ENABLE = false;

    /** APNS certificate path */
    public static final String APNS_CERT_PATH = "HoccerTalkApplePushNotificationDev.p12";

    /** APNS certificate password */
    public static final String APNS_CERT_PASSWORD = "Mf)LW*L#/\"dlco\\/;ns{";

    /** Put APNS into sandbox mode */
    public static final boolean APNS_USE_SANDBOX = true;

}
