package com.lzw;

import com.alibaba.fastjson.JSON;
import com.lzw.entity.User;
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.AbstractHighlighterBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.Highlighter;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class EsApiApplicationTests {

    @Autowired
    @Qualifier("getHightClient")
    private RestHighLevelClient testClient;
    @Test
    void createIndex() throws IOException {
        //??????????????????
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("ling_index");
        //??????????????????
        CreateIndexResponse createIndexResponse = testClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);
    }

    @Test
    void getIndex() throws IOException {
        //????????????????????????
        GetIndexRequest ling_index = new GetIndexRequest("ling_index");
        //????????????
        boolean exists = testClient.indices().exists(ling_index, RequestOptions.DEFAULT);
        if(exists){
            //??????????????????
            GetIndexResponse getIndexResponse = testClient.indices().get(ling_index, RequestOptions.DEFAULT);
            System.out.println("?????????"+getIndexResponse);
        }
    }

    @Test
    void getIndex1() throws IOException {
        //????????????????????????
        GetIndexRequest ling_index = new GetIndexRequest("ling_index1");
        //????????????
        boolean exists = testClient.indices().exists(ling_index, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    @Test
    void deleteIndex() throws IOException {
        //????????????????????????
        DeleteIndexRequest ling_index = new DeleteIndexRequest("ling_index");
        //????????????
        AcknowledgedResponse delete = testClient.indices().delete(ling_index, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }

    @Test
    void createDoc() throws IOException {
        //????????????
        User user = new User("?????????", 3);
        //????????????
        IndexRequest indexRequest = new IndexRequest("ling_index");
        //??????
        indexRequest.id("1");
        indexRequest.timeout(TimeValue.timeValueSeconds(1));
        indexRequest.timeout("1s");
        //?????????????????????
        IndexRequest source = indexRequest.source(JSON.toJSONString(user), XContentType.JSON);
        //?????????????????????,????????????
        IndexResponse index = testClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(index.toString());
        System.out.println(index.status());
    }


    @Test
    void getDocIfExists() throws IOException {
        GetRequest ling_index = new GetRequest("ling_index","6");
        //??????????????????_source?????????
//        ling_index.fetchSourceContext(new FetchSourceContext(false));
//        ling_index.storedFields("name");
        boolean exists = testClient.exists(ling_index, RequestOptions.DEFAULT);
        if(exists){
            System.out.println("??????");
            //??????????????????
            GetResponse documentFields = testClient.get(ling_index, RequestOptions.DEFAULT);
            //??????????????????
            System.out.println(documentFields.getSourceAsString());
            System.out.println(documentFields);
        }
    }

    @Test
    void updateDoc() throws IOException {
        UpdateRequest ling_index = new UpdateRequest("ling_index", "1");
        ling_index.timeout("1s");
        //????????????
        User user = new User("?????????JAVA", 3);
        UpdateRequest doc = ling_index.doc(JSON.toJSONString(user), XContentType.JSON);
        UpdateResponse update = testClient.update(ling_index, RequestOptions.DEFAULT);
        System.out.println(update.status());
        System.out.println(update);
    }

    @Test
    void deleteDoc() throws IOException {
        DeleteRequest ling_index = new DeleteRequest("ling_index", "5");
        ling_index.timeout("1s");
        DeleteResponse delete = testClient.delete(ling_index, RequestOptions.DEFAULT);
        System.out.println(delete.status());
        System.out.println(delete);
    }

    @Test
    void bulkRequest() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");

        List<User> users = new ArrayList<>();
        users.add(new User("lzw1", 1));
        users.add(new User("lzw2", 2));
        users.add(new User("lzw3", 3));
        users.add(new User("ass1", 4));
        users.add(new User("ass2", 5));
        users.add(new User("ass3", 6));
        users.add(new User("ass", 7));

        for (int i = 0; i < users.size(); i++) {
            IndexRequest indexRequest = new IndexRequest("ling_index");
            indexRequest.id(""+(i+1));
            indexRequest.source(JSON.toJSONString(users.get(i)), XContentType.JSON);
            bulkRequest.add(indexRequest);
        }
        BulkResponse bulk = testClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulk.status());
        System.out.println(bulk.hasFailures());
        System.out.println(bulk);
    }

    @Test
    //SearchRequest ????????????
    //SearchSourceBuilder ????????????
    //HighlightBuilder ????????????
    //TermQueryBuilder  ??????
    //MatchAllQueryBuilder  ??????
    //
    void testSearch() throws IOException {
        SearchRequest ling_index = new SearchRequest("ling_index");
        //????????????
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //??????QueryBuilders??????
        //????????????
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "lzw1");
        searchSourceBuilder.query(termQueryBuilder);
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("name");
        searchSourceBuilder.highlighter(highlightBuilder);
//??????
//        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
//        searchSourceBuilder.from();
//        searchSourceBuilder.size();
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        ling_index.source(searchSourceBuilder);
        SearchResponse search = testClient.search(ling_index, RequestOptions.DEFAULT);
        System.out.println(search);
        System.out.println(search.status());
        System.out.println(search.getHits());
        System.out.println(JSON.toJSONString(search.getHits()));
        for (SearchHit hit : search.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

}
