/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package it.polimi.ist.flink.streaming.datasource;


import it.polimi.ist.flink.streaming.datasource.source.TwitterFilterEndpoint;
import it.polimi.ist.flink.streaming.datasource.transformations.JsonToTweetMap;
import it.polimi.ist.flink.streaming.datasource.utils.Tweet;
import it.polimi.ist.flink.streaming.datasource.utils.TwitterUtils;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import java.util.ArrayList;
import java.util.List;

/**
 * Exercise on Twitter 1
 * EventTime Time
 * Window Function
 */
public class TwitterEx3 {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        System.out.println("Usage: TwitterExample [--output <path>] " +
                "[--twitter-source.consumerKey <key> --twitter-source.consumerSecret <secret> --twitter-source.token <token> --twitter-source.tokenSecret <tokenSecret>]");

        // set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        env.setParallelism(params.getInt("parallelism", 1));

        // get input data
        DataStream<String> streamSource;
        if (params.has(TwitterSource.CONSUMER_KEY) &&
                params.has(TwitterSource.CONSUMER_SECRET) &&
                params.has(TwitterSource.TOKEN) &&
                params.has(TwitterSource.TOKEN_SECRET)
                ) {
            TwitterSource source = new TwitterSource(params.getProperties());

            source.setCustomEndpointInitializer(new TwitterFilterEndpoint(new String[]{"sanders"}));
            streamSource = env.addSource(source);
        } else {
            System.out.println("Executing TwitterStream example with default props.");
            System.out.println("Use --twitter-source.consumerKey <key> --twitter-source.consumerSecret <secret> " +
                    "--twitter-source.token <token> --twitter-source.tokenSecret <tokenSecret> specify the authentication info.");
            // get default test text data
            streamSource = env.fromElements(TwitterUtils.TEXTS);
        }

        final SingleOutputStreamOperator<Tweet> tweets = streamSource
                .map(new JsonToTweetMap());


        SplitStream<Tweet> split_tweet_stream = tweets.split(new OutputSelector<Tweet>() {
            public Iterable<String> select(Tweet tweet) {
                List<String> out = new ArrayList<String>();
                if (tweet.retweeted) {
                    out.add("retweets");
                } else {
                    out.add("tweets");
                }
                return out;
            }
        });

        KeyedStream<Tweet, Tuple> keyed_retweets = split_tweet_stream.select("retweets").keyBy("re_id");
        KeyedStream<Tweet, Tuple> keyed_tweets = split_tweet_stream.select("tweets").keyBy("id");

        DataStream<Tuple2<String, String>> joined_stream = keyed_tweets.join(keyed_retweets).where(new KeySelector<Tweet, String>() {
            public String getKey(Tweet tweet) throws Exception {
                return tweet.id;
            }
        }).equalTo(new KeySelector<Tweet, String>() {
            public String getKey(Tweet tweet) throws Exception {
                return tweet.re_id;
            }
        }).window(TumblingProcessingTimeWindows.of(Time.seconds(30))).apply(new JoinFunction<Tweet, Tweet, Tuple2<String, String>>() {
            public Tuple2<String, String> join(Tweet tweet, Tweet tweet2) throws Exception {
                return new Tuple2<String, String>(tweet.id, tweet2.re_id);
            }
        });


        joined_stream.map(new MapFunction<Tuple2<String, String>, Tuple2<String, Integer>>() {
            public Tuple2<String, Integer> map(Tuple2<String, String> t) throws Exception {
                return new Tuple2<String, Integer>(t.f0, 1);
            }
        }).keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(30))).sum(1).print();

        env.execute("TwitterEx3 Streaming Example");
    }


}
