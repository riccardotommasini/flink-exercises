package it.polimi.ist.flink.streaming.datasource.transformations;

// *************************************************************************
// USER FUNCTIONS
// *************************************************************************

import it.polimi.ist.flink.streaming.datasource.utils.TwitterUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.codehaus.jackson.JsonNode;

/**
 * Deserialize JSON from twitter source
 * <p/>
 * <p/>
 * Implements a string tokenizer that splits sentences into words as a
 * user-defined FlatMapFunction. The function takes a line (String) and
 * splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
 * Integer>}).
 */
public class SelectTweetBody implements FlatMapFunction<String, Tuple3<String, String, String>> {
    private static final long serialVersionUID = 1L;


    /**
     * Select the language from the incoming JSON text
     */
    public void flatMap(String value, Collector<Tuple3<String, String, String>> out) throws Exception {
        JsonNode tweet = TwitterUtils.getTweet(value);
        if (tweet.has("text") && tweet.has("user") && tweet.get("user").has("lang")) {
            out.collect(new Tuple3<String, String, String>(tweet.get("user").get("id").asText(),
                    tweet.get("user").get("lang").asText(), tweet.get("text").asText()));
        }
    }
}
