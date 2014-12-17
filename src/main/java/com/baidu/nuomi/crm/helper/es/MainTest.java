package com.baidu.nuomi.crm.helper.es;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.*;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created with IntelliJ IDEA.
 * User: mazhen01
 * Date: 2014/12/12
 * Time: 17:54
 */
public class MainTest {

    private static final String INDEX_NAME = "nuomi_crm";
    private static final String TYPE_NAME = "firm_index";
    private static final int STATION_COUNT = 50;
    private static final int DOCUMENT_COUNT = 2000000;
    private static final boolean isDebug = false;

    public static void main(String[] args) {
        MainTest test = new MainTest();
        //test.doInsert();
        //test.doBulkInsert();
        //test.search();

//        test.searchMatch();
//        test.searchFilter();
//        test.searchMultiple();
//        test.orderMultiple();

        try {
            test.doSearch();
        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    public void doInsert() {
        IndexResponse response = ESClient.getClient().prepareIndex(INDEX_NAME, TYPE_NAME)
                .setSource(buildDocument()).execute().actionGet();
        System.out.println(response.toString());
    }

    private Map<String, Object> buildDocument() {
        Random random = new Random();
        int id = random.nextInt(DOCUMENT_COUNT);
        Map<String, Object> document = new HashMap<String, Object>(64);
        document.put("firmId", id);
        document.put("firmName", "覆雪清泉" + id);
        document.put("dealCount", id);
        document.put("categoryName", "静观勿扰");
        document.put("originFlag", 1);
        document.put("level", 10);
        document.put("staffId", id);
        document.put("stationId", random.nextInt(STATION_COUNT));
        document.put("stationName", Thread.currentThread().getName());
        document.put("createDate", new Date());
        document.put("phone", 110);
        document.put("desc", "我知道你不知道我知道你不知道");

        return document;
    }

    public void deleteType() {
        long startTime = System.currentTimeMillis();
        QueryBuilder builder = QueryBuilders.matchAllQuery();
        DeleteByQueryResponse response = ESClient.getClient().prepareDeleteByQuery(INDEX_NAME).setTypes(TYPE_NAME)
                .setQuery(builder).execute().actionGet();
        System.out.println("delete used time:" + (System.currentTimeMillis() - startTime));
    }

    public void doBulkInsert() {

        deleteType();

        for (int loop = 0; loop < 100; loop++) {

            BulkRequestBuilder bulkRequest = ESClient.getClient().prepareBulk();

            for (int i = 0; i < (DOCUMENT_COUNT / 100); i++) {
                IndexRequest aRequest = ESClient.getClient().prepareIndex(INDEX_NAME, TYPE_NAME)
                        .setSource(buildDocument()).request();
                bulkRequest.add(aRequest);
            }
            BulkResponse bulkResponse = bulkRequest.execute().actionGet();
            System.out.println("used time:" + bulkResponse.getTook());
            System.out.println("insert hasFailures:" + bulkResponse.hasFailures());
        }

    }

    public void search() {
//        QueryBuilder query = QueryBuilders.termQuery("level", 10);
//        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
//        boolQuery.must(null);
        QueryBuilder query = QueryBuilders.matchQuery("firmName", "清泉");
        FilterBuilder stationFilter = FilterBuilders.termFilter("stationId", 5);
        FilterBuilder staffFilter = FilterBuilders.termFilter("staffId", 936);
        query = QueryBuilders.filteredQuery(query, FilterBuilders.andFilter(stationFilter).add(staffFilter));
        SortBuilder dealSorter = SortBuilders.fieldSort("dealCount").order(SortOrder.ASC);

//        AndFilterBuilder and = FilterBuilders.andFilter();
        SearchResponse response = ESClient.getClient().prepareSearch(INDEX_NAME)
                .setPreference("_local")
                .setTypes(TYPE_NAME)
                .setFrom(0).setSize(10)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(query)
                .addSort(dealSorter)
                .setExplain(true)
                .execute().actionGet();
        SearchHits hits = response.getHits();
        System.out.println("used time:" + response.getTook());
        for (SearchHit hit : hits) {
            Map<String, Object> source = hit.getSource();
            System.out.println(source.toString());
        }
    }

    public void doSearch() throws Exception{
        ExecutorService executorService = new ThreadPoolExecutor(50, 50, 0L, TimeUnit.MILLISECONDS,
                new SynchronousQueue<Runnable>(), new ThreadPoolExecutor.CallerRunsPolicy());
        long totalStart = System.currentTimeMillis();
        System.out.println("doSearch start");

        int dataCount = 2000;
        for (int i =0; i < dataCount; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    //searchMatch();
                    //searchFilter();
                    orderFilter();
                    //searchMultiple();
                    //orderMultiple();
                }
            });
        }


        System.out.println("total cost:" + (System.currentTimeMillis() - totalStart));
        System.out.println("doSearch end");
        Thread.sleep(5000);
        executorService.shutdown();
        ESClient.getClient().close();
        System.out.println("doSearch close");
    }

    public void searchMatch() {
        QueryBuilder query = buildFirmNameQuery();

        SearchResponse response = buildSearchRequestBuilder().setQuery(query).execute().actionGet();
        SearchHits hits = response.getHits();

        System.out.println("count: " + response.getHits().getTotalHits() + ", took:" + response.getTookInMillis());
        printHits(query, hits);

    }

    public void searchFilter() {
        QueryBuilder query = QueryBuilders.matchAllQuery();
        query = QueryBuilders.filteredQuery(query, buildFilterBuilder());
        SearchResponse response = buildSearchRequestBuilder().setQuery(query).execute().actionGet();
        SearchHits hits = response.getHits();

        System.out.println("count: " + response.getHits().getTotalHits() + ", took:" + response.getTookInMillis());
        printHits(query, hits);
    }

    public void orderFilter() {
        QueryBuilder query = QueryBuilders.matchAllQuery();
        query = QueryBuilders.filteredQuery(query, buildFilterBuilder());
        SearchResponse response = buildSearchRequestBuilder().setQuery(query).addSort(buildSortBuilder("firmId")).execute().actionGet();
        SearchHits hits = response.getHits();

        System.out.println("count: " + response.getHits().getTotalHits() + ", took:" + response.getTookInMillis());
        printHits(query, hits);
    }

    public void searchMultiple() {
        QueryBuilder query = buildFirmNameQuery();
        query = QueryBuilders.filteredQuery(query, buildFilterBuilder());
        SearchResponse response = buildSearchRequestBuilder().setQuery(query).execute().actionGet();
        SearchHits hits = response.getHits();

        System.out.println("count: " + response.getHits().getTotalHits() + ", took:" + response.getTookInMillis());
        printHits(query, hits);
    }

    public void orderMultiple() {
        QueryBuilder query = buildFirmNameQuery();
        query = QueryBuilders.filteredQuery(query, buildFilterBuilder());
        SearchResponse response = buildSearchRequestBuilder().setQuery(query).addSort(buildSortBuilder("firmId")).execute().actionGet();
        SearchHits hits = response.getHits();

        System.out.println("count: " + response.getHits().getTotalHits() + ", took:" + response.getTookInMillis());
        printHits(query, hits);
    }

    private void printHits(QueryBuilder query, SearchHits hits) {

        if (!isDebug) {
            return;
        }

        System.out.println(query.toString());

        for (SearchHit hit : hits) {
            Map<String, Object> source = hit.getSource();
            System.out.println(source.toString());
        }
    }

    private QueryBuilder buildFirmNameQuery() {
        Random r = new Random();
        String key = "清泉" + r.nextInt(10000);
        return QueryBuilders.matchQuery("firmName", key);
    }

    private SearchRequestBuilder buildSearchRequestBuilder() {
        return ESClient.getClient().prepareSearch(INDEX_NAME)
                .setPreference("_local")
                .setTypes(TYPE_NAME)
                .setFrom(15).setSize(10)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setExplain(true);
    }

    private FilterBuilder buildFilterBuilder() {
        Random random = new Random();
        int stationId = random.nextInt(STATION_COUNT);
        int dealCount = random.nextInt(DOCUMENT_COUNT);

        FilterBuilder stationFilter = FilterBuilders.termFilter("stationId", stationId);
        FilterBuilder staffFilter = FilterBuilders.rangeFilter("dealCount").gt(dealCount);
        return FilterBuilders.andFilter(stationFilter).add(staffFilter).cache(true);
    }


    private SortBuilder buildSortBuilder(String field) {
        return SortBuilders.fieldSort(field).order(SortOrder.DESC);
    }
}
