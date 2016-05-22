package it.polimi.ist.flink.streaming.datasource.transformations;

// *************************************************************************
// USER FUNCTIONS
// *************************************************************************

import it.polimi.ist.flink.streaming.datasource.utils.TwitterUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.codehaus.jackson.JsonNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Deserialize JSON from twitter source
 * <p/>
 * <p/>
 * Implements a string tokenizer that splits sentences into words as a
 * user-defined FlatMapFunction. The function takes a line (String) and
 * splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
 * Integer>}).
 */
public class SelectTweetHashtag implements FlatMapFunction<String, Tuple2<String, String[]>> {
    private static final long serialVersionUID = 1L;


    /**
     * Select the language from the incoming JSON text
     */
    public void flatMap(String value, Collector<Tuple2<String, String[]>> out) throws Exception {
        JsonNode tweet = TwitterUtils.getTweet(value);
        if (tweet.has("entities" )) {
            JsonNode entities = tweet.get("entities" );
            if (entities.has("hashtags" ) && entities.get("hashtags" ).isArray()) {

                Iterator<JsonNode> hashtags = entities.get("hashtags" ).getElements();
                List<String> hashtag_list = new ArrayList<String>();
                while (hashtags.hasNext()) {
                    hashtag_list.add(hashtags.next().get("text" ).asText());
                }

                String[] a = new String[hashtag_list.size()];
                a = hashtag_list.toArray(a);
                out.collect(new Tuple2<String, String[]>(tweet.get("user").get("id").asText(),a));
            }
        }
    }
}
