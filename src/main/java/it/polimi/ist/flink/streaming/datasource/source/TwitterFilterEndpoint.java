package it.polimi.ist.flink.streaming.datasource.source;

import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by Riccardo on 22/05/16.
 */
public class TwitterFilterEndpoint implements TwitterSource.EndpointInitializer, Serializable {

    private String[] hashtags;

    public TwitterFilterEndpoint(String[] hashtags) {
        this.hashtags=hashtags;
    }

    public StreamingEndpoint createEndpoint() {
        StatusesFilterEndpoint statusesFilterEndpoint = new StatusesFilterEndpoint();
        statusesFilterEndpoint.trackTerms(Arrays.asList(hashtags));
        //statusesFilterEndpoint.languages(Arrays.asList(new String[]{"en"}));
        statusesFilterEndpoint.stallWarnings(false);
        statusesFilterEndpoint.delimited(false);
        return statusesFilterEndpoint;
    }

}
