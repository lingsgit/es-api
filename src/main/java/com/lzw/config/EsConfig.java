package com.lzw.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EsConfig {
    @Bean
    public RestHighLevelClient getHightClient(){
        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("192.168.213.123",9200,"http")));
    }
}
