package com.bfd.controller;

import com.bfd.utils.Constants;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.ResultsExtractor;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.elasticsearch.client.Client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsQuery;

@RestController
@ResponseBody
@Slf4j
public class Test {


    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    @Autowired
    Client client;

    @Value("${es.rms_device_info.indexName}")
    private String indexDeviceName;

    @Value("${es.rms_device_info.indexType}")
    private String indexDeviceType;

    /**
     * 简化版ES聚合
     * 习思想传播中心-统计查询--用户使用统计（查询Es设备数）
     *
     */
    @RequestMapping("getTests")
    private Map<Long,Long> getTerminal(){

        //创建返回的Map集合
        Map<Long,Long> map = Maps.newHashMap();

        TermsBuilder typeAgg = AggregationBuilders
                .terms(Constants.BUSINESSID)  //取名字
                .field(Constants.BUSINESSID); //聚合属性

        SearchResponse response = client.prepareSearch(indexDeviceName).setPreference("_primary")
//                .setTypes(indexDeviceType)
                .addAggregation(typeAgg)
                .execute().actionGet();
        StringTerms stringTerms  = response.getAggregations().get(Constants.BUSINESSID);
        List<Terms.Bucket> buckets = stringTerms.getBuckets();
        for (int i = 0; i < buckets.size(); i++) {
            Terms.Bucket bucket = buckets.get(i);
            long id = bucket.getKeyAsNumber().longValue();
            long docCount = bucket.getDocCount();
            map.put(id,docCount);
        }
        return map;
    }

    /**
     * 习思想传播中心-统计查询--用户使用统计（查询Es设备数，包含去重）
     *
     */
    @RequestMapping("getTest")
    private Map<Long,Long> getTerminal2(){

        List<Long> businessIds = new ArrayList<>();
        businessIds.add(334L);
        businessIds.add(336L);
        businessIds.add(337L);
        businessIds.add(345L);
        businessIds.add(346L);
        businessIds.add(347L);
        businessIds.add(348L);
        businessIds.add(349L);
        businessIds.add(339L);

        //创建返回的Map集合
        Map<Long,Long> map = Maps.newHashMap();

        //聚合
        TermsBuilder termAggs =  AggregationBuilders
                .terms(Constants.BUSINESSID)
                .field(Constants.BUSINESSID)
                .size(Constants.ES_ZERO);

        CardinalityBuilder cardinalityAggs = AggregationBuilders
                .cardinality(Constants.DEVICEID)
                .field(Constants.DEVICEID);

        termAggs.subAggregation(cardinalityAggs);

        // 创建查询语句
        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withIndices(indexDeviceName)
                .withTypes(indexDeviceType)
                .withFields(Constants.BUSINESSID)
                .withQuery(boolQuery()
                        .must(termsQuery(Constants.BUSINESSID,businessIds)))
                .withPageable(PageRequest.of(0, 1))
                .addAggregation(termAggs)
                .build();
        System.out.println("==============="+searchQuery.getQuery());
        Aggregations aggregations = elasticsearchTemplate.query(searchQuery, new ResultsExtractor<Aggregations>() {
            @Override
            public Aggregations extract(SearchResponse response) {
                return response.getAggregations();
            }
        });
        StringTerms stringTerms = (StringTerms)aggregations.asMap().get(Constants.BUSINESSID);
        List<Terms.Bucket> buckets = stringTerms.getBuckets();
        for (int i = 0; i < buckets.size(); i++) {
            Terms.Bucket bucket = buckets.get(i);
            long id = bucket.getKeyAsNumber().longValue();
            long docCount = bucket.getDocCount();
            Cardinality cardinality = bucket.getAggregations().get(Constants.DEVICEID);
            map.put(id,cardinality.getValue());
//            map.put(id,docCount);
        }
        return map;
    }

}

// es 聚合查询语句
//    {
//    "query": {
//        "bool": {
//            "must": {
//                "terms": {
//                    "businessId": [
//                        334,
//                        336,
//                        337,
//                        339,
//                        345,
//                        346,
//                        347,
//                        348,
//                        349
//                    ]
//                        }
//                    }
//                }
//            },
//        "size": 0,
//        "aggs": {
//            "businessId": {
//                "terms": {
//                    "field": "businessId",
//                    "size": 0
//                },
//                "aggs": {
//                    "deviceId": {
//                        "cardinality": {
//                            "field": "deviceId"
//                           }
//                    }
//                }
//            }
//        }
//    }