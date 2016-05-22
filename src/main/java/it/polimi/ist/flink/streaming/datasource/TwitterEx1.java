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
import it.polimi.ist.flink.streaming.datasource.transformations.FilterByLang;
import it.polimi.ist.flink.streaming.datasource.transformations.JsonToTweetMap;
import it.polimi.ist.flink.streaming.datasource.utils.TwitterUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

/**
 * Exercise on Twitter 1
 * Processing Time
 * Map Function
 * Flat Map Function
 * Filter Function
 */
public class TwitterEx1 {

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

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

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

        streamSource
                .map(new JsonToTweetMap())
                .filter(new FilterByLang("en" ))
                //.flatMap(new SelectTweetBody())
               /* .filter(new FilterFunction<Tuple2<String, String[]>>() {
                    public boolean filter(Tuple2<String, String[]> stringTuple2) throws Exception {
                        return Arrays.asList(stringTuple2.f1).size() > 0;
                    }
                })*/

                .print();

        // execute program
        env.execute("TwitterEx1 Streaming Example");
    }


}
