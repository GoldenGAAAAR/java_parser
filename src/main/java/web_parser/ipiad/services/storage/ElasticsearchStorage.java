package web_parser.ipiad.services.storage;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import web_parser.ipiad.entity.NewsEntity;
import web_parser.ipiad.services.runnable.NewsLoader;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ElasticsearchStorage {
    private final ObjectMapper objectMapper;
    private final RestClient restClient;
    private final ElasticsearchTransport transport;
    private final ElasticsearchClient elasticClient;
    private final String indexName;
    private final static Logger log = LoggerFactory.getLogger(NewsLoader.class);

    public ElasticsearchStorage(String elasticUrl, String idxName) {
        objectMapper = JsonMapper.builder().build();
        restClient = RestClient.builder(HttpHost.create(elasticUrl)).build();
        transport = new RestClientTransport(restClient, new JacksonJsonpMapper(objectMapper));
        elasticClient = new ElasticsearchClient(transport);
        indexName = idxName;
        log.info("Elastic connection established!");
    }

    public void createIndexIfNotExists() throws IOException {
        BooleanResponse indexExists = elasticClient.indices().exists(ex -> ex.index(indexName));
        log.info(elasticClient.indices().stats().toString());
        if (indexExists.value()) {
            log.info("Index exists: " + indexName);
            return;
        }
        elasticClient.indices().create(c -> c.index(indexName).mappings(m -> m
                .properties("id", p -> p.text(d -> d.fielddata(true)))
                .properties("header", p -> p.text(d -> d.fielddata(true)))
                .properties("text", p -> p.text(d -> d.fielddata(true)))
                .properties("URL", p -> p.text(d -> d.fielddata(true)))
                .properties("theme", p -> p.text(d -> d.fielddata(true)))
                .properties("time", p -> p.text(d -> d.fielddata(true)))
                .properties("hash", p -> p.text(d -> d.fielddata(true)))
        ));
        log.info("Created index: " + indexName);
    }

    public void insertData(NewsEntity news) {
        try {
            IndexResponse response = elasticClient.index(i -> i.index(indexName).document(news));
            log.info("Page from: " + news.getURL() + " was added to elastic");
        } catch (IOException e) {
            log.error("Error with inserting page to elastic from: " + news.getURL());
            log.error(e.getMessage());
        }
    }

    public boolean checkExistence(String hashValue) {
        SearchResponse<NewsEntity> response = null;
        try {
            response = elasticClient.search(s -> s
                            .index(indexName)
                            .query(q -> q.match(t -> t.field("hash").query(hashValue))),
                    NewsEntity.class
            );
        } catch (IOException e) {
            log.error("Error with check existence for -> " + hashValue);
            System.exit(1);
        }
        return response.hits().total().value() != 0;
    }
}