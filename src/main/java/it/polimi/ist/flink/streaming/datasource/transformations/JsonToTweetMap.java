package it.polimi.ist.flink.streaming.datasource.transformations;

import it.polimi.ist.flink.streaming.datasource.utils.Tweet;
import it.polimi.ist.flink.streaming.datasource.utils.TwitterUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.codehaus.jackson.JsonNode;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Riccardo on 22/05/16.
 */
public class JsonToTweetMap implements MapFunction<String, Tweet> {

    public final DateFormat df = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy");

    public Tweet map(String s) throws Exception {
        JsonNode tweet = TwitterUtils.getTweet(s);
        Tweet t = new Tweet();


        JsonNode user = tweet.get("user");
        if (tweet.has("user")) {
            t.user_id = user.get("id").asText();
            t.user_name = user.get("name").asText();
            if (user.has("lang")) {
                t.lang = user.get("lang").asText();
            }
        }

        if (tweet.has("id_str")) {
            t.id = tweet.get("id_str").asText();
        }

        if (tweet.has("text")) {
            t.content = tweet.get("text").asText();
        }

        if (tweet.has("favorited")) {
            t.favorited = tweet.get("favorited").asBoolean();
        }

        if (t.retweeted = tweet.has("retweeted_status")) {
            JsonNode retweet = tweet.get("retweeted_status");
            if (retweet.has("id_str")) {
                t.re_id = tweet.get("id_str").asText();
            }
        }


        if (tweet.has("created_at")) {
            t.timestamp = df.parse(tweet.get("created_at").asText());
        }


        if (tweet.has("entities")) {
            JsonNode entities = tweet.get("entities");

            List<String> list;
            if (entities.has("hashtags") && entities.get("hashtags").isArray()) {

                Iterator<JsonNode> hashtags = entities.get("hashtags").getElements();
                list = new ArrayList<String>();
                while (hashtags.hasNext()) {
                    list.add(hashtags.next().get("text").asText());
                }

                String[] a = new String[list.size()];
                t.hashtags = list.toArray(a);
            }


            if (entities.has("urls") && entities.get("urls").isArray()) {

                Iterator<JsonNode> hashtags = entities.get("urls").getElements();
                list = new ArrayList<String>();
                while (hashtags.hasNext()) {
                    list.add(hashtags.next().get("url").asText());
                }

                String[] a = new String[list.size()];
                t.urls = list.toArray(a);
            }

            if (entities.has("user_mentions") && entities.get("user_mentions").isArray()) {

                Iterator<JsonNode> hashtags = entities.get("user_mentions").getElements();
                list = new ArrayList<String>();
                while (hashtags.hasNext()) {
                    list.add(hashtags.next().get("id").asText());
                }

                String[] a = new String[list.size()];
                t.mentions = list.toArray(a);
            }


        }


        return t;

    }
}
