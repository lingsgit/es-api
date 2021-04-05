package com.lzw;

import com.alibaba.fastjson.JSON;
import com.lzw.entity.User;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
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
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

@SpringBootTest
class EsApiApplicationTests {

    @Autowired
    @Qualifier("getHightClient")
    private RestHighLevelClient testClient;
    @Test
    void createIndex() throws IOException {
        //创建索引请求
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("ling_index");
        //执行创建请求
        CreateIndexResponse createIndexResponse = testClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);
    }

    @Test
    void getIndex() throws IOException {
        //创建获取索引请求
        GetIndexRequest ling_index = new GetIndexRequest("ling_index");
        //是否存在
        boolean exists = testClient.indices().exists(ling_index, RequestOptions.DEFAULT);
        if(exists){
            //执行创建请求
            GetIndexResponse getIndexResponse = testClient.indices().get(ling_index, RequestOptions.DEFAULT);
            System.out.println("存在："+getIndexResponse);
        }
    }

    @Test
    void getIndex1() throws IOException {
        //创建获取索引请求
        GetIndexRequest ling_index = new GetIndexRequest("ling_index1");
        //是否存在
        boolean exists = testClient.indices().exists(ling_index, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    @Test
    void deleteIndex() throws IOException {
        //创建删除索引请求
        DeleteIndexRequest ling_index = new DeleteIndexRequest("ling_index");
        //是否存在
        AcknowledgedResponse delete = testClient.indices().delete(ling_index, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }

    @Test
    void createDoc() throws IOException {
        //创建对象
        User user = new User("令责问", 3);
        //创建请求
        IndexRequest indexRequest = new IndexRequest("ling_index");
        //规则
        indexRequest.id("1");
        indexRequest.timeout(TimeValue.timeValueSeconds(1));
        indexRequest.timeout("1s");
        //将数据放入请求
        IndexRequest source = indexRequest.source(JSON.toJSONString(user), XContentType.JSON);
        //客户端发送请求,获取响应
        IndexResponse index = testClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(index.toString());
        System.out.println(index.status());
    }


    @Test
    void getDocIfExists() throws IOException {
        GetRequest ling_index = new GetRequest("ling_index","1");
        //不获取返回的_source上下文
//        ling_index.fetchSourceContext(new FetchSourceContext(false));
//        ling_index.storedFields("name");
        boolean exists = testClient.exists(ling_index, RequestOptions.DEFAULT);
        if(exists){
            System.out.println("存在");
            //获取文档信息
            GetResponse documentFields = testClient.get(ling_index, RequestOptions.DEFAULT);
            //打印文档内容
            System.out.println(documentFields.getSourceAsString());
            System.out.println(documentFields);
        }
    }

    @Test
    void updateDoc() throws IOException {
        UpdateRequest ling_index = new UpdateRequest("ling_index", "1");
        ling_index.timeout("1s");
        //创建对象
        User user = new User("令责问JAVA", 3);
        UpdateRequest doc = ling_index.doc(JSON.toJSONString(user), XContentType.JSON);
        UpdateResponse update = testClient.update(ling_index, RequestOptions.DEFAULT);
        System.out.println(update.status());
        System.out.println(update);
    }

    @Test
    void deleteDoc() throws IOException {
        DeleteRequest ling_index = new DeleteRequest("ling_index", "1");
        ling_index.timeout("1s");
        DeleteResponse delete = testClient.delete(ling_index, RequestOptions.DEFAULT);
        System.out.println(delete.status());
        System.out.println(delete);
    }
}
