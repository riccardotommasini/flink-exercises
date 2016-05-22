package it.polimi.ist.flink.streaming.datasource.transformations;

import it.polimi.ist.flink.streaming.datasource.utils.Tweet;
import it.polimi.ist.flink.streaming.datasource.utils.TwitterUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.codehaus.jackson.JsonNode;

/**
 * Created by Riccardo on 20/05/16.
 */
public class FilterByLang implements FilterFunction<Tweet> {

    private final String lang;

    public FilterByLang(String lang) {
        this.lang = lang;
    }

    public boolean filter(Tweet t) throws Exception {

        return lang.equals(t.lang);
    }
}
