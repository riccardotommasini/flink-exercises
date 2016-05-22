package it.polimi.ist.flink.streaming.datasource.utils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;

/**
 * Created by Riccardo on 22/05/16.
 */
public class Tweet implements Serializable {


    public String id, re_id, user_id, user_name;
    public Date timestamp;
    public String content;
    public String lang;
    public String[] hashtags;
    public String[] mentions;


    public String[] urls;
    public boolean retweeted, favorited;

    @Override
    public String toString() {
        return "Tweet{" +
                "id='" + id + '\'' +
                ", re_id='" + re_id + '\'' +
                ", user_id='" + user_id + '\'' +
                ", user_name='" + user_name + '\'' +
                ", timestamp=" + timestamp +
                ", content='" + content + '\'' +
                ", lang='" + lang + '\'' +
                ", hashtags=" + Arrays.toString(hashtags) +
                ", mentions=" + Arrays.toString(mentions) +
                ", urls=" + Arrays.toString(urls) +
                ", retweeted=" + retweeted +
                ", favorited=" + favorited +
                '}';
    }
}
