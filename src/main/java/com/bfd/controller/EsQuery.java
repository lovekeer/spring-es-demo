package com.bfd.test;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.lucene.analysis.miscellaneous.WordDelimiterFilterFactory.TYPES;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

@RestController
@ResponseBody
@Slf4j
public class EsQuery {

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    @Autowired
    Client client;

    @Value("${es.rms_device_info.indexName}")
    private String indexDeviceName;

    @Value("${es.rms_device_info.indexType}")
    private String indexDeviceType;

    @RequestMapping("getOne")
    private Object getOne(){

        // 构造query查询器实例
        QueryBuilder query = termQuery("businessId", "336");
        System.out.println(String.format("{\"query\":%s}", query));

        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withIndices(indexDeviceName)
                .withTypes(indexDeviceType)
                .withPageable(new PageRequest(0, 10))
                .withQuery(query)//
                .build();

        // 获取es查询结果，以Page接收并返回，---分页使用（配合withPageable使用，如不设置分页数，则默认查询前10条）
//        Page<JSONObject> queryForPage = elasticsearchTemplate.queryForPage(searchQuery, JSONObject.class);
//        return queryForPage.getContent();

        // 获取es查询结果，以list接收并返回（如果结果含多条记录，则默认返回前10条，除非设置withPageable的size返回条数）
        List<JSONObject> result = elasticsearchTemplate.queryForList(searchQuery, JSONObject.class);
        return result;
    }

    @RequestMapping("getTwo")
    private Object getTwo(){

        // 构造query查询器实例
        QueryBuilder query = termQuery("businessId", "336");
        System.out.println(String.format("{\"query\":%s}", query));

        SearchResponse response = client.prepareSearch(indexDeviceName).setTypes(indexDeviceType)
                .setQuery(query)
                .setFrom(0).setSize(10)  // 分页(默认0,10)，非必填
                .setExplain(true) // 是否按查询匹配度排序，非必填
                .addSort("createTime", SortOrder.DESC) // 按照创建时间降序排列，非必填
//                .addFields(fields) // 指定返回字段，非必填
                .execute()
                .actionGet();

        // 第一种获取结果方式：
        SearchHits searchHits = response.getHits();
        SearchHit[] hits = searchHits.getHits();  // 结果集

        // 查询出多少条文档
        System.out.println("总数："+searchHits.getTotalHits());

        for (int i = 0; i < searchHits.getHits().length; i++) {
           System.out.println("_id：" + searchHits.getAt(i).getId());
            System.out.println("json：" + searchHits.getAt(i).getSourceAsString()); // getSource()结果是map，getSourceAsString结果是json
        }

        // 第二种获取结果方式：
        for(SearchHit hit: hits) {
            // 如果设置了返回字段fields，则不能使用hit.getSource()，source整个为null
            System.out.println("source.deviceId: " + hit.getSource().get("deviceId"));
            System.out.println("source.createTime: " + hit.getSource().get("createTime"));
            System.out.println("source.businessId: " + hit.getSource().get("businessId"));
        }

        JSONArray array = new JSONArray();
        for (SearchHit hit : hits) {
            String json = hit.getSourceAsString();
            array.add(json);
        }
        return array;
    }

    @RequestMapping("getThree")
    private Object getThree(){

        String[] fields = {"createTime","businessId","deviceId"};

        // 构造query查询器实例
        QueryBuilder query = termQuery("businessId", "336");
        System.out.println(String.format("{\"query\":%s}", query));

        SearchResponse response = client.prepareSearch(indexDeviceName).setTypes(indexDeviceType)
                .setQuery(query)
                .setFrom(0).setSize(10)  // 分页(默认0,10)，非必填
                .setExplain(true) // 是否按查询匹配度排序，非必填
                .addSort("createTime", SortOrder.DESC) // 按照创建时间降序排列，非必填
                .addFields(fields) // 指定返回字段，非必填
                .execute()
                .actionGet();

        SearchHits searchHits = response.getHits();
        SearchHit[] hits = searchHits.getHits();  // 结果集

        // 查询出多少条文档
        System.out.println("总数："+searchHits.getTotalHits());

        // 第一种获取结果方式：
        for (int i = 0; i < searchHits.getHits().length; i++) {
            System.out.println("_id：" + searchHits.getAt(i).getId());
            System.out.println("json：" + searchHits.getAt(i).getSourceAsString()); // getSource()结果是map，getSourceAsString结果是json
        }

        // 第二种获取结果方式：遍历结果集
        for(SearchHit hit: hits) {
            if (hit.getFields().containsKey("deviceId")) {
                System.out.println("field.deviceId: "+ hit.getFields().get("deviceId").getValue());
            }
            if (hit.getFields().containsKey("createTime")) {
                System.out.println("field.createTime: "+ hit.getFields().get("createTime").getValue());
            }
            if (hit.getFields().containsKey("businessId")) {
                System.out.println("field.businessId: "+ hit.getFields().get("businessId").getValue());
            }
        }

        JSONArray array = new JSONArray();
        for (SearchHit hit : hits) {
            String json = hit.getSourceAsString();
            array.add(json);
        }
        return array;
    }

    @RequestMapping("getFour")
    private Object getFour(){

        // 构造query查询器实例
        QueryBuilder query = termQuery("businessId", "336");
        System.out.println(String.format("{\"query\":%s}", query));

        SearchResponse response = client.prepareSearch(indexDeviceName).setTypes(indexDeviceType)
                .setQuery(query)
                .execute()
                .actionGet();

        SearchHits searchHits = response.getHits();
        System.out.println("总数："+searchHits.getTotalHits());
        SearchHit[] hits = searchHits.getHits();
        JSONArray array = new JSONArray();
        for (SearchHit hit : hits) {
            String json = hit.getSourceAsString();
            System.out.println(hit.getScore());
            System.out.println(json);
            array.add(json);
        }
        return array;
    }



}
