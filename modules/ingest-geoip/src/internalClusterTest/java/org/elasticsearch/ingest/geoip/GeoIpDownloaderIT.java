/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import com.maxmind.geoip2.DatabaseReader;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.ingest.SimulateDocumentBaseResult;
import org.elasticsearch.action.ingest.SimulatePipelineRequest;
import org.elasticsearch.action.ingest.SimulatePipelineResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.After;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPInputStream;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@ClusterScope(scope = Scope.TEST, maxNumDataNodes = 1)
public class GeoIpDownloaderIT extends AbstractGeoIpIT {

    private static final String ENDPOINT = System.getProperty("geoip_endpoint");

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ReindexPlugin.class, IngestGeoIpPlugin.class, GeoIpProcessorNonIngestNodeIT.IngestGeoIpSettingsPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal));
        if (ENDPOINT != null) {
            settings.put(GeoIpDownloader.ENDPOINT_SETTING.getKey(), ENDPOINT);
        }
        return settings.build();
    }

    @After
    public void disableDownloader(){
        ClusterUpdateSettingsResponse settingsResponse = client().admin().cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), false))
            .get();
        assertTrue(settingsResponse.isAcknowledged());
    }

    public void testGeoIpDatabasesDownload() throws Exception {
        ClusterUpdateSettingsResponse settingsResponse = client().admin().cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), true))
            .get();
        assertTrue(settingsResponse.isAcknowledged());
        assertBusy(() -> {
            PersistentTasksCustomMetadata.PersistentTask<PersistentTaskParams> task = getTask();
            assertNotNull(task);
            GeoIpTaskState state = (GeoIpTaskState) task.getState();
            assertNotNull(state);
            assertEquals(Set.of("GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb"), state.getDatabases().keySet());
        }, 2, TimeUnit.MINUTES);

        GeoIpTaskState state = (GeoIpTaskState) getTask().getState();
        for (String id : List.of("GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb")) {
            assertBusy(() -> {
                GeoIpTaskState.Metadata metadata = state.get(id);
                BoolQueryBuilder queryBuilder = new BoolQueryBuilder()
                    .filter(new MatchQueryBuilder("name", id))
                    .filter(new RangeQueryBuilder("chunk")
                        .from(metadata.getFirstChunk())
                        .to(metadata.getLastChunk(), true));
                int size = metadata.getLastChunk() - metadata.getFirstChunk() + 1;
                SearchResponse res = client().prepareSearch(GeoIpDownloader.DATABASES_INDEX)
                    .setSize(size)
                    .setQuery(queryBuilder)
                    .addSort("chunk", SortOrder.ASC)
                    .get();
                TotalHits totalHits = res.getHits().getTotalHits();
                assertEquals(TotalHits.Relation.EQUAL_TO, totalHits.relation);
                assertEquals(size, totalHits.value);
                assertEquals(size, res.getHits().getHits().length);

                List<byte[]> data = new ArrayList<>();

                for (SearchHit hit : res.getHits().getHits()) {
                    data.add((byte[]) hit.getSourceAsMap().get("data"));
                }

                GZIPInputStream stream = new GZIPInputStream(new MultiByteArrayInputStream(data));
                Path tempFile = createTempFile();
                try (OutputStream os = new BufferedOutputStream(Files.newOutputStream(tempFile, TRUNCATE_EXISTING, WRITE, CREATE))) {
                    stream.transferTo(os);
                }

                parseDatabase(tempFile);
            });
        }
    }

    @TestLogging(value = "org.elasticsearch.ingest.geoip:TRACE", reason = "https://github.com/elastic/elasticsearch/issues/69972")
    public void testUseGeoIpProcessorWithDownloadedDBs() throws Exception {
        // setup:
        BytesReference bytes;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            {
                builder.startArray("processors");
                {
                    builder.startObject();
                    {
                        builder.startObject("geoip");
                        {
                            builder.field("field", "ip");
                            builder.field("target_field", "ip-city");
                            builder.field("database_file", "GeoLite2-City.mmdb");
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                    builder.startObject();
                    {
                        builder.startObject("geoip");
                        {
                            builder.field("field", "ip");
                            builder.field("target_field", "ip-country");
                            builder.field("database_file", "GeoLite2-Country.mmdb");
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                    builder.startObject();
                    {
                        builder.startObject("geoip");
                        {
                            builder.field("field", "ip");
                            builder.field("target_field", "ip-asn");
                            builder.field("database_file", "GeoLite2-ASN.mmdb");
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endArray();
            }
            builder.endObject();
            bytes = BytesReference.bytes(builder);
        }
        assertAcked(client().admin().cluster().preparePutPipeline("_id", bytes, XContentType.JSON).get());

        // verify before updating dbs
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.startArray("docs");
            {
                builder.startObject();
                builder.field("_index", "my-index");
                {
                    builder.startObject("_source");
                    builder.field("ip", "89.160.20.128");
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            bytes = BytesReference.bytes(builder);
        }
        SimulatePipelineRequest simulateRequest = new SimulatePipelineRequest(bytes, XContentType.JSON);
        simulateRequest.setId("_id");
        {
            SimulatePipelineResponse simulateResponse = client().admin().cluster().simulatePipeline(simulateRequest).actionGet();
            assertThat(simulateResponse.getPipelineId(), equalTo("_id"));
            assertThat(simulateResponse.getResults().size(), equalTo(1));
            SimulateDocumentBaseResult result = (SimulateDocumentBaseResult) simulateResponse.getResults().get(0);
            assertThat(result.getIngestDocument().getFieldValue("ip-city.city_name", String.class), equalTo("Tumba"));
            assertThat(result.getIngestDocument().getFieldValue("ip-asn.organization_name", String.class), equalTo("Bredband2 AB"));
            assertThat(result.getIngestDocument().getFieldValue("ip-country.country_name", String.class), equalTo("Sweden"));
        }

        // Enable downloader:
        Settings.Builder settings = Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), true);
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings));

        final List<Path> geoipTmpDirs = StreamSupport.stream(internalCluster().getInstances(Environment.class).spliterator(), false)
            .map(env -> {
                Path geoipTmpDir = env.tmpFile().resolve("geoip-databases");
                assertThat(Files.exists(geoipTmpDir), is(true));
                return geoipTmpDir;
            }).collect(Collectors.toList());
        assertBusy(() -> {
            for (Path geoipTmpDir : geoipTmpDirs) {
                try (Stream<Path> list = Files.list(geoipTmpDir)) {
                    List<String> files = list.map(Path::toString).collect(Collectors.toList());
                    assertThat(files, containsInAnyOrder(endsWith("GeoLite2-City.mmdb"), endsWith("GeoLite2-Country.mmdb"),
                        endsWith("GeoLite2-ASN.mmdb")));
                }
            }
        });

        // Verify after updating dbs:
        assertBusy(() -> {
            SimulatePipelineResponse simulateResponse = client().admin().cluster().simulatePipeline(simulateRequest).actionGet();
            assertThat(simulateResponse.getPipelineId(), equalTo("_id"));
            assertThat(simulateResponse.getResults().size(), equalTo(1));
            SimulateDocumentBaseResult result = (SimulateDocumentBaseResult) simulateResponse.getResults().get(0);
            assertThat(result.getIngestDocument().getFieldValue("ip-city.city_name", String.class), equalTo("Linköping"));
            assertThat(result.getIngestDocument().getFieldValue("ip-asn.organization_name", String.class), equalTo("Bredband2 AB"));
            assertThat(result.getIngestDocument().getFieldValue("ip-country.country_name", String.class), equalTo("Sweden"));
        });

        // Disable downloader:
        settings = Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), false);
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings));

        assertBusy(() -> {
            for (Path geoipTmpDir : geoipTmpDirs) {
                try (Stream<Path> list = Files.list(geoipTmpDir)) {
                    List<String> files = list.map(Path::toString).collect(Collectors.toList());
                    assertThat(files, empty());
                }
            }
        });
    }

    @SuppressForbidden(reason = "Maxmind API requires java.io.File")
    private void parseDatabase(Path tempFile) throws IOException {
        try (DatabaseReader databaseReader = new DatabaseReader.Builder(tempFile.toFile()).build()) {
            assertNotNull(databaseReader.getMetadata());
        }
    }

    private PersistentTasksCustomMetadata.PersistentTask<PersistentTaskParams> getTask() {
        return PersistentTasksCustomMetadata.getTaskWithId(clusterService().state(), GeoIpDownloader.GEOIP_DOWNLOADER);
    }

    private static class MultiByteArrayInputStream extends InputStream {

        private final Iterator<byte[]> data;
        private ByteArrayInputStream current;

        private MultiByteArrayInputStream(List<byte[]> data) {
            this.data = data.iterator();
        }

        @Override
        public int read() {
            if (current == null) {
                if (data.hasNext() == false) {
                    return -1;
                }

                current = new ByteArrayInputStream(data.next());
            }
            int read = current.read();
            if (read == -1) {
                current = null;
                return read();
            }
            return read;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (current == null) {
                if (data.hasNext() == false) {
                    return -1;
                }

                current = new ByteArrayInputStream(data.next());
            }
            int read = current.read(b, off, len);
            if (read == -1) {
                current = null;
                return read(b, off, len);
            }
            return read;
        }
    }
}
