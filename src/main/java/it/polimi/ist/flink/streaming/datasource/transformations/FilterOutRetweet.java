package it.polimi.ist.flink.streaming.datasource.transformations;

import it.polimi.ist.flink.streaming.datasource.utils.Tweet;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * Created by Riccardo on 20/05/16.
 */
public class FilterOutRetweet implements FilterFunction<Tweet> {

    private final String lang;

    public FilterOutRetweet(String lang) {
        this.lang = lang;
    }

    public boolean filter(Tweet t) throws Exception {

        return !t.retweeted;
    }
}
