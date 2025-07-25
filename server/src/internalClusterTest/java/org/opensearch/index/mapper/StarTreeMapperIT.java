/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Rounding;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.Index;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.compositeindex.CompositeIndexSettings;
import org.opensearch.index.compositeindex.datacube.DataCubeDateTimeUnit;
import org.opensearch.index.compositeindex.datacube.DateDimension;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.NumericDimension;
import org.opensearch.index.compositeindex.datacube.OrdinalDimension;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeIndexSettings;
import org.opensearch.index.compositeindex.datacube.startree.utils.date.DateTimeUnitAdapter;
import org.opensearch.index.compositeindex.datacube.startree.utils.date.DateTimeUnitRounding;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.SearchHit;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Integration tests for star tree mapper
 */
public class StarTreeMapperIT extends OpenSearchIntegTestCase {
    private static final String TEST_INDEX = "test";
    Settings settings = Settings.builder()
        .put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true)
        .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), true)
        .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(512, ByteSizeUnit.MB))
        .build();

    private static XContentBuilder createMinimalTestMapping(boolean invalidDim, boolean invalidMetric, boolean wildcard) {
        try {
            return jsonBuilder().startObject()
                .startObject("composite")
                .startObject("startree-1")
                .field("type", "star_tree")
                .startObject("config")
                .startObject("date_dimension")
                .field("name", "timestamp")
                .endObject()
                .startArray("ordered_dimensions")
                .startObject()
                .field("name", getDim(invalidDim, wildcard))
                .endObject()
                .startObject()
                .field("name", "keyword_dv")
                .endObject()
                .startObject()
                .field("name", "unsignedLongDimension") // UnsignedLongDimension
                .endObject()
                .endArray()
                .startArray("metrics")
                .startObject()
                .field("name", getMetric(invalidMetric, false))
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .endObject()
                .startObject("properties")
                .startObject("timestamp")
                .field("type", "date")
                .endObject()
                .startObject("numeric_dv")
                .field("type", "integer")
                .field("doc_values", true)
                .endObject()
                .startObject("numeric")
                .field("type", "integer")
                .field("doc_values", false)
                .endObject()
                .startObject("keyword_dv")
                .field("type", "keyword")
                .field("doc_values", true)
                .endObject()
                .startObject("keyword")
                .field("type", "keyword")
                .field("doc_values", false)
                .endObject()
                .startObject("ip_no_dv")
                .field("type", "ip")
                .field("doc_values", false)
                .endObject()
                .startObject("ip")
                .field("type", "ip")
                .field("doc_values", true)
                .endObject()
                .startObject("wildcard")
                .field("type", "wildcard")
                .field("doc_values", false)
                .endObject()
                .startObject("unsignedLongDimension")
                .field("type", "unsigned_long")
                .field("doc_values", true)
                .endObject()
                .endObject()
                .endObject();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static XContentBuilder createNestedTestMapping() {
        try {
            return jsonBuilder().startObject()
                .startObject("composite")
                .startObject("startree-1")
                .field("type", "star_tree")
                .startObject("config")
                .startObject("date_dimension")
                .field("name", "timestamp")
                .endObject()
                .startArray("ordered_dimensions")
                .startObject()
                .field("name", "nested.nested1.status")
                .endObject()
                .startObject()
                .field("name", "nested.nested1.keyword_dv")
                .endObject()
                .startObject()
                .field("name", "nested.nested1.name.keyword1")
                .endObject()
                .startObject()
                .field("name", "nested.nested1.name.keyword2")
                .endObject()
                .endArray()
                .startArray("metrics")
                .startObject()
                .field("name", "nested3.numeric_dv")
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .endObject()
                .startObject("properties")
                .startObject("timestamp")
                .field("type", "date")
                .endObject()
                .startObject("nested3")
                .startObject("properties")
                .startObject("numeric_dv")
                .field("type", "integer")
                .field("doc_values", true)
                .endObject()
                .endObject()
                .endObject()
                .startObject("numeric")
                .field("type", "integer")
                .field("doc_values", false)
                .endObject()
                .startObject("nested")
                .startObject("properties")
                .startObject("nested1")
                .startObject("properties")
                .startObject("status")
                .field("type", "integer")
                .field("doc_values", true)
                .endObject()
                .startObject("name")
                .field("type", "text")
                .startObject("fields")
                .startObject("keyword1")
                .field("type", "keyword")
                .field("ignore_above", 512)
                .endObject()
                .startObject("keyword2")
                .field("type", "keyword")
                .field("ignore_above", 512)
                .endObject()
                .endObject()
                .endObject()
                .startObject("keyword_dv")
                .field("type", "keyword")
                .field("doc_values", true)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .startObject("nested-not-startree")
                .startObject("properties")
                .startObject("nested1")
                .startObject("properties")
                .startObject("status")
                .field("type", "integer")
                .field("doc_values", true)
                .endObject()
                .startObject("keyword_dv")
                .field("type", "keyword")
                .field("doc_values", true)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .startObject("keyword")
                .field("type", "keyword")
                .field("doc_values", false)
                .endObject()
                .startObject("ip")
                .field("type", "ip")
                .field("doc_values", false)
                .endObject()
                .endObject()
                .endObject();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static XContentBuilder createNestedTestMappingForArray() {
        try {
            return jsonBuilder().startObject()
                .startObject("composite")
                .startObject("startree-1")
                .field("type", "star_tree")
                .startObject("config")
                .startObject("date_dimension")
                .field("name", "timestamp")
                .endObject()
                .startArray("ordered_dimensions")
                .startObject()
                .field("name", "status")
                .endObject()
                .startObject()
                .field("name", "nested.nested1.keyword_dv")
                .endObject()
                .endArray()
                .startArray("metrics")
                .startObject()
                .field("name", "nested3.numeric_dv")
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .endObject()
                .startObject("properties")
                .startObject("timestamp")
                .field("type", "date")
                .endObject()
                .startObject("status")
                .field("type", "integer")
                .endObject()
                .startObject("nested3")
                .startObject("properties")
                .startObject("numeric_dv")
                .field("type", "integer")
                .field("doc_values", true)
                .endObject()
                .endObject()
                .endObject()
                .startObject("numeric")
                .field("type", "integer")
                .field("doc_values", false)
                .endObject()
                .startObject("nested")
                .startObject("properties")
                .startObject("nested1")
                .startObject("properties")
                .startObject("status")
                .field("type", "integer")
                .field("doc_values", true)
                .endObject()
                .startObject("keyword_dv")
                .field("type", "keyword")
                .field("doc_values", true)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .startObject("nested-not-startree")
                .startObject("properties")
                .startObject("nested1")
                .startObject("properties")
                .startObject("status")
                .field("type", "integer")
                .field("doc_values", true)
                .endObject()
                .startObject("keyword_dv")
                .field("type", "keyword")
                .field("doc_values", true)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .startObject("keyword")
                .field("type", "keyword")
                .field("doc_values", false)
                .endObject()
                .startObject("ip")
                .field("type", "ip")
                .field("doc_values", false)
                .endObject()
                .endObject()
                .endObject();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static XContentBuilder createDateTestMapping(boolean duplicate) {
        try {
            return jsonBuilder().startObject()
                .startObject("composite")
                .startObject("startree-1")
                .field("type", "star_tree")
                .startObject("config")
                .startObject("date_dimension")
                .field("name", "timestamp")
                .startArray("calendar_intervals")
                .value("day")
                .value("quarter-hour")
                .value(duplicate ? "quarter-hour" : "half-hour")
                .endArray()
                .endObject()
                .startArray("ordered_dimensions")
                .startObject()
                .field("name", "numeric_dv")
                .endObject()
                .endArray()
                .startArray("metrics")
                .startObject()
                .field("name", "numeric_dv")
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .endObject()
                .startObject("properties")
                .startObject("timestamp")
                .field("type", "date")
                .endObject()
                .startObject("numeric_dv")
                .field("type", "integer")
                .field("doc_values", true)
                .endObject()
                .startObject("numeric")
                .field("type", "integer")
                .field("doc_values", false)
                .endObject()
                .startObject("keyword_dv")
                .field("type", "keyword")
                .field("doc_values", true)
                .endObject()
                .startObject("keyword")
                .field("type", "keyword")
                .field("doc_values", false)
                .endObject()
                .endObject()
                .endObject();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static XContentBuilder createMaxDimTestMapping() {
        try {
            return jsonBuilder().startObject()
                .startObject("composite")
                .startObject("startree-1")
                .field("type", "star_tree")
                .startObject("config")
                .startObject("date_dimension")
                .field("name", "timestamp")
                .startArray("calendar_intervals")
                .value("day")
                .value("month")
                .value("half-hour")
                .endArray()
                .endObject()
                .startArray("ordered_dimensions")
                .startObject()
                .field("name", "dim2")
                .endObject()
                .startObject()
                .field("name", "dim3")
                .endObject()
                .endArray()
                .startArray("metrics")
                .startObject()
                .field("name", "dim2")
                .endObject()
                .startObject()
                .field("name", "dim3")
                .endObject()
                .startObject()
                .field("name", "dim4")
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .endObject()
                .startObject("properties")
                .startObject("timestamp")
                .field("type", "date")
                .endObject()
                .startObject("dim2")
                .field("type", "integer")
                .field("doc_values", true)
                .endObject()
                .startObject("dim3")
                .field("type", "integer")
                .field("doc_values", true)
                .endObject()
                .startObject("dim4")
                .field("type", "integer")
                .field("doc_values", true)
                .endObject()
                .endObject()
                .endObject();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static XContentBuilder createTestMappingWithoutStarTree() {
        try {
            return jsonBuilder().startObject()
                .startObject("properties")
                .startObject("timestamp")
                .field("type", "date")
                .endObject()
                .startObject("numeric_dv")
                .field("type", "integer")
                .field("doc_values", true)
                .endObject()
                .startObject("numeric")
                .field("type", "integer")
                .field("doc_values", false)
                .endObject()
                .startObject("keyword_dv")
                .field("type", "keyword")
                .field("doc_values", true)
                .endObject()
                .startObject("keyword")
                .field("type", "keyword")
                .field("doc_values", false)
                .endObject()
                .endObject()
                .endObject();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static XContentBuilder createUpdateTestMapping(boolean changeDim, boolean sameStarTree) {
        try {
            return jsonBuilder().startObject()
                .startObject("composite")
                .startObject(sameStarTree ? "startree-1" : "startree-2")
                .field("type", "star_tree")
                .startObject("config")
                .startObject("date_dimension")
                .field("name", "timestamp")
                .endObject()
                .startArray("ordered_dimensions")
                .startObject()
                .field("name", changeDim ? "numeric_new" : getDim(false, false))
                .endObject()
                .endArray()
                .startArray("metrics")
                .startObject()
                .field("name", getDim(false, false))
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .endObject()
                .startObject("properties")
                .startObject("timestamp")
                .field("type", "date")
                .endObject()
                .startObject("numeric_dv")
                .field("type", "integer")
                .field("doc_values", true)
                .endObject()
                .startObject("numeric")
                .field("type", "integer")
                .field("doc_values", false)
                .endObject()
                .startObject("numeric_new")
                .field("type", "integer")
                .field("doc_values", true)
                .endObject()
                .startObject("keyword_dv")
                .field("type", "keyword")
                .field("doc_values", true)
                .endObject()
                .startObject("keyword")
                .field("type", "keyword")
                .field("doc_values", false)
                .endObject()
                .endObject()
                .endObject();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private XContentBuilder getMappingWithDuplicateFields(boolean isDuplicateDim, boolean isDuplicateMetric) {
        XContentBuilder mapping = null;
        try {
            mapping = jsonBuilder().startObject()
                .startObject("composite")
                .startObject("startree-1")
                .field("type", "star_tree")
                .startObject("config")
                .startObject("date_dimension")
                .field("name", "timestamp")
                .endObject()
                .startArray("ordered_dimensions")
                .startObject()
                .field("name", "numeric_dv")
                .endObject()
                .startObject()
                .field("name", isDuplicateDim ? "numeric_dv" : "numeric_dv1")  // Duplicate dimension
                .endObject()
                .endArray()
                .startArray("metrics")
                .startObject()
                .field("name", "numeric_dv")
                .endObject()
                .startObject()
                .field("name", isDuplicateMetric ? "numeric_dv" : "numeric_dv1")  // Duplicate metric
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .endObject()
                .startObject("properties")
                .startObject("timestamp")
                .field("type", "date")
                .endObject()
                .startObject("numeric_dv")
                .field("type", "integer")
                .field("doc_values", true)
                .endObject()
                .startObject("numeric_dv1")
                .field("type", "integer")
                .field("doc_values", true)
                .endObject()
                .endObject()
                .endObject();
        } catch (IOException e) {
            fail("Failed to create mapping: " + e.getMessage());
        }
        return mapping;
    }

    private static String getDim(boolean hasDocValues, boolean isWildCard) {
        if (hasDocValues) {
            return random().nextBoolean() ? "numeric" : random().nextBoolean() ? "keyword" : "ip_no_dv";
        } else if (isWildCard) {
            return "wildcard";
        }
        return "numeric_dv";
    }

    private static String getMetric(boolean hasDocValues, boolean isKeyword) {
        if (hasDocValues) {
            return "numeric";
        } else if (isKeyword) {
            return "ip";
        }
        return "numeric_dv";
    }

    @Before
    public final void setupNodeSettings() {
        Settings request = Settings.builder().put(CompositeIndexSettings.STAR_TREE_INDEX_ENABLED_SETTING.getKey(), true).build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(request).get());
    }

    public void testValidCompositeIndex() {
        prepareCreate(TEST_INDEX).setSettings(settings).setMapping(createMinimalTestMapping(false, false, false)).get();
        Iterable<IndicesService> dataNodeInstances = internalCluster().getDataNodeInstances(IndicesService.class);
        for (IndicesService service : dataNodeInstances) {
            final Index index = resolveIndex("test");
            if (service.hasIndex(index)) {
                IndexService indexService = service.indexService(index);
                Set<CompositeMappedFieldType> fts = indexService.mapperService().getCompositeFieldTypes();

                for (CompositeMappedFieldType ft : fts) {
                    assertFalse(ft == null);
                    assertTrue(ft.unwrap() instanceof StarTreeMapper.StarTreeFieldType);
                    StarTreeMapper.StarTreeFieldType starTreeFieldType = (StarTreeMapper.StarTreeFieldType) ft;
                    assertEquals("timestamp", starTreeFieldType.getDimensions().get(0).getField());
                    assertTrue(starTreeFieldType.getDimensions().get(0) instanceof DateDimension);
                    DateDimension dateDim = (DateDimension) starTreeFieldType.getDimensions().get(0);
                    List<DateTimeUnitRounding> expectedTimeUnits = Arrays.asList(
                        new DateTimeUnitAdapter(Rounding.DateTimeUnit.MINUTES_OF_HOUR),
                        DataCubeDateTimeUnit.HALF_HOUR_OF_DAY
                    );
                    for (int i = 0; i < dateDim.getSortedCalendarIntervals().size(); i++) {
                        assertEquals(expectedTimeUnits.get(i).shortName(), dateDim.getSortedCalendarIntervals().get(i).shortName());
                    }
                    assertEquals(4, starTreeFieldType.getDimensions().size());
                    assertEquals("numeric_dv", starTreeFieldType.getDimensions().get(1).getField());
                    assertEquals("keyword_dv", starTreeFieldType.getDimensions().get(2).getField());
                    assertEquals("unsignedLongDimension", starTreeFieldType.getDimensions().get(3).getField());

                    assertEquals("numeric_dv", starTreeFieldType.getMetrics().get(0).getField());
                    List<MetricStat> expectedMetrics = Arrays.asList(MetricStat.VALUE_COUNT, MetricStat.SUM, MetricStat.AVG);
                    assertEquals(expectedMetrics, starTreeFieldType.getMetrics().get(0).getMetrics());
                    assertEquals(10000, starTreeFieldType.getStarTreeConfig().maxLeafDocs());
                    assertEquals(
                        StarTreeFieldConfiguration.StarTreeBuildMode.OFF_HEAP,
                        starTreeFieldType.getStarTreeConfig().getBuildMode()
                    );
                    assertEquals(Collections.emptySet(), starTreeFieldType.getStarTreeConfig().getSkipStarNodeCreationInDims());
                }
            }
        }
    }

    public void testValidCompositeIndexWithDates() {
        prepareCreate(TEST_INDEX).setMapping(createDateTestMapping(false)).setSettings(settings).get();
        Iterable<IndicesService> dataNodeInstances = internalCluster().getDataNodeInstances(IndicesService.class);
        for (IndicesService service : dataNodeInstances) {
            final Index index = resolveIndex("test");
            if (service.hasIndex(index)) {
                IndexService indexService = service.indexService(index);
                Set<CompositeMappedFieldType> fts = indexService.mapperService().getCompositeFieldTypes();

                for (CompositeMappedFieldType ft : fts) {
                    assertFalse(ft == null);
                    assertTrue(ft.unwrap() instanceof StarTreeMapper.StarTreeFieldType);
                    StarTreeMapper.StarTreeFieldType starTreeFieldType = (StarTreeMapper.StarTreeFieldType) ft;
                    assertEquals("timestamp", starTreeFieldType.getDimensions().get(0).getField());
                    assertTrue(starTreeFieldType.getDimensions().get(0) instanceof DateDimension);
                    DateDimension dateDim = (DateDimension) starTreeFieldType.getDimensions().get(0);
                    List<DateTimeUnitRounding> expectedTimeUnits = Arrays.asList(
                        DataCubeDateTimeUnit.QUARTER_HOUR_OF_DAY,
                        DataCubeDateTimeUnit.HALF_HOUR_OF_DAY,
                        new DateTimeUnitAdapter(Rounding.DateTimeUnit.DAY_OF_MONTH)
                    );
                    for (int i = 0; i < dateDim.getIntervals().size(); i++) {
                        assertEquals(expectedTimeUnits.get(i).shortName(), dateDim.getSortedCalendarIntervals().get(i).shortName());
                    }
                    assertEquals("numeric_dv", starTreeFieldType.getDimensions().get(1).getField());
                    assertEquals("numeric_dv", starTreeFieldType.getMetrics().get(0).getField());
                    List<MetricStat> expectedMetrics = Arrays.asList(MetricStat.VALUE_COUNT, MetricStat.SUM, MetricStat.AVG);
                    assertEquals(expectedMetrics, starTreeFieldType.getMetrics().get(0).getMetrics());
                    assertEquals(10000, starTreeFieldType.getStarTreeConfig().maxLeafDocs());
                    assertEquals(
                        StarTreeFieldConfiguration.StarTreeBuildMode.OFF_HEAP,
                        starTreeFieldType.getStarTreeConfig().getBuildMode()
                    );
                    assertEquals(Collections.emptySet(), starTreeFieldType.getStarTreeConfig().getSkipStarNodeCreationInDims());
                }
            }
        }
    }

    public void testValidCompositeIndexWithNestedFields() {
        prepareCreate(TEST_INDEX).setMapping(createNestedTestMapping()).setSettings(settings).get();
        Iterable<IndicesService> dataNodeInstances = internalCluster().getDataNodeInstances(IndicesService.class);
        for (IndicesService service : dataNodeInstances) {
            final Index index = resolveIndex("test");
            if (service.hasIndex(index)) {
                IndexService indexService = service.indexService(index);
                Set<CompositeMappedFieldType> fts = indexService.mapperService().getCompositeFieldTypes();

                for (CompositeMappedFieldType ft : fts) {
                    assertFalse(ft == null);
                    assertTrue(ft.unwrap() instanceof StarTreeMapper.StarTreeFieldType);
                    StarTreeMapper.StarTreeFieldType starTreeFieldType = (StarTreeMapper.StarTreeFieldType) ft;
                    assertEquals("timestamp", starTreeFieldType.getDimensions().get(0).getField());
                    assertTrue(starTreeFieldType.getDimensions().get(0) instanceof DateDimension);
                    DateDimension dateDim = (DateDimension) starTreeFieldType.getDimensions().get(0);
                    List<DateTimeUnitRounding> expectedTimeUnits = Arrays.asList(
                        new DateTimeUnitAdapter(Rounding.DateTimeUnit.MINUTES_OF_HOUR),
                        DataCubeDateTimeUnit.HALF_HOUR_OF_DAY
                    );
                    for (int i = 0; i < dateDim.getIntervals().size(); i++) {
                        assertEquals(expectedTimeUnits.get(i).shortName(), dateDim.getSortedCalendarIntervals().get(i).shortName());
                    }
                    assertEquals("nested.nested1.status", starTreeFieldType.getDimensions().get(1).getField());
                    assertTrue(starTreeFieldType.getDimensions().get(1) instanceof NumericDimension);
                    assertEquals("nested.nested1.keyword_dv", starTreeFieldType.getDimensions().get(2).getField());
                    assertTrue(starTreeFieldType.getDimensions().get(2) instanceof OrdinalDimension);
                    assertEquals("nested.nested1.name.keyword1", starTreeFieldType.getDimensions().get(3).getField());
                    assertTrue(starTreeFieldType.getDimensions().get(3) instanceof OrdinalDimension);
                    assertEquals("nested.nested1.name.keyword2", starTreeFieldType.getDimensions().get(4).getField());
                    assertTrue(starTreeFieldType.getDimensions().get(4) instanceof OrdinalDimension);
                    assertEquals("nested3.numeric_dv", starTreeFieldType.getMetrics().get(0).getField());
                    List<MetricStat> expectedMetrics = Arrays.asList(MetricStat.VALUE_COUNT, MetricStat.SUM, MetricStat.AVG);
                    assertEquals(expectedMetrics, starTreeFieldType.getMetrics().get(0).getMetrics());
                    assertEquals(10000, starTreeFieldType.getStarTreeConfig().maxLeafDocs());
                    assertEquals(
                        StarTreeFieldConfiguration.StarTreeBuildMode.OFF_HEAP,
                        starTreeFieldType.getStarTreeConfig().getBuildMode()
                    );
                    assertEquals(Collections.emptySet(), starTreeFieldType.getStarTreeConfig().getSkipStarNodeCreationInDims());
                }
            }
        }
    }

    public void testValidCompositeIndexWithDuplicateDates() {
        prepareCreate(TEST_INDEX).setMapping(createDateTestMapping(true)).setSettings(settings).get();
        Iterable<IndicesService> dataNodeInstances = internalCluster().getDataNodeInstances(IndicesService.class);
        for (IndicesService service : dataNodeInstances) {
            final Index index = resolveIndex("test");
            if (service.hasIndex(index)) {
                IndexService indexService = service.indexService(index);
                Set<CompositeMappedFieldType> fts = indexService.mapperService().getCompositeFieldTypes();

                for (CompositeMappedFieldType ft : fts) {
                    assertFalse(ft == null);
                    assertTrue(ft.unwrap() instanceof StarTreeMapper.StarTreeFieldType);
                    StarTreeMapper.StarTreeFieldType starTreeFieldType = (StarTreeMapper.StarTreeFieldType) ft;
                    assertEquals("timestamp", starTreeFieldType.getDimensions().get(0).getField());
                    assertTrue(starTreeFieldType.getDimensions().get(0) instanceof DateDimension);
                    DateDimension dateDim = (DateDimension) starTreeFieldType.getDimensions().get(0);
                    List<DateTimeUnitRounding> expectedTimeUnits = Arrays.asList(
                        DataCubeDateTimeUnit.QUARTER_HOUR_OF_DAY,
                        new DateTimeUnitAdapter(Rounding.DateTimeUnit.DAY_OF_MONTH)
                    );
                    for (int i = 0; i < dateDim.getIntervals().size(); i++) {
                        assertEquals(expectedTimeUnits.get(i).shortName(), dateDim.getSortedCalendarIntervals().get(i).shortName());
                    }
                    assertEquals("numeric_dv", starTreeFieldType.getDimensions().get(1).getField());
                    assertEquals(2, starTreeFieldType.getMetrics().size());
                    assertEquals("numeric_dv", starTreeFieldType.getMetrics().get(0).getField());

                    // Assert default metrics
                    List<MetricStat> expectedMetrics = Arrays.asList(MetricStat.VALUE_COUNT, MetricStat.SUM, MetricStat.AVG);
                    assertEquals(expectedMetrics, starTreeFieldType.getMetrics().get(0).getMetrics());

                    assertEquals("_doc_count", starTreeFieldType.getMetrics().get(1).getField());
                    assertEquals(List.of(MetricStat.DOC_COUNT), starTreeFieldType.getMetrics().get(1).getMetrics());

                    assertEquals(10000, starTreeFieldType.getStarTreeConfig().maxLeafDocs());
                    assertEquals(
                        StarTreeFieldConfiguration.StarTreeBuildMode.OFF_HEAP,
                        starTreeFieldType.getStarTreeConfig().getBuildMode()
                    );
                    assertEquals(Collections.emptySet(), starTreeFieldType.getStarTreeConfig().getSkipStarNodeCreationInDims());
                }
            }
        }
    }

    public void testCompositeIndexWithIndexNotSpecified() {
        Settings settings = Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(512, ByteSizeUnit.MB))
            .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), true)
            .build();
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> prepareCreate(TEST_INDEX).setSettings(settings).setMapping(createMinimalTestMapping(false, false, false)).get()
        );
        assertEquals(
            "Failed to parse mapping [_doc]: Set 'index.composite_index' as true as part of index settings to use star tree index",
            ex.getMessage()
        );
    }

    public void testAppendOnlyInCompositeIndexNotSpecified() {
        Settings settings = Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(512, ByteSizeUnit.MB))
            .put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true)
            .build();
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> prepareCreate(TEST_INDEX).setSettings(settings).setMapping(createMinimalTestMapping(false, false, false)).get()
        );
        assertEquals(
            "Failed to parse mapping [_doc]: Set 'index.append_only.enabled' as true as part of index settings to use star tree index",
            ex.getMessage()
        );
    }

    public void testCompositeIndexWithHigherTranslogFlushSize() {
        Settings settings = Settings.builder()
            .put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), true)
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(513, ByteSizeUnit.MB))
            .build();
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> prepareCreate(TEST_INDEX).setSettings(settings).setMapping(createMinimalTestMapping(false, false, false)).get()
        );
        assertEquals("You can configure 'index.translog.flush_threshold_size' with upto '512mb' for composite index", ex.getMessage());
    }

    public void testCompositeIndexWithArraysInCompositeField() throws IOException {
        prepareCreate(TEST_INDEX).setSettings(settings).setMapping(createMinimalTestMapping(false, false, false)).get();
        // Attempt to index a document with an array field
        XContentBuilder doc = jsonBuilder().startObject()
            .field("timestamp", "2023-06-01T12:00:00Z")
            .startArray("numeric_dv")
            .value(10)
            .value(20)
            .value(30)
            .endArray()
            .endObject();

        // Index the document and refresh
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> client().prepareIndex(TEST_INDEX).setSource(doc).get()
        );
        assertEquals(
            "object mapping for [_doc] with array for [numeric_dv] cannot be accepted, as the field is also part of composite index mapping which does not accept arrays",
            ex.getMessage()
        );
    }

    public void testCompositeIndexWithArraysInNestedCompositeField() throws IOException {
        // here nested.nested1.status is part of the composite field but "nested" field itself is an array
        prepareCreate(TEST_INDEX).setSettings(settings).setMapping(createNestedTestMapping()).get();
        // Attempt to index a document with an array field
        final XContentBuilder doc = jsonBuilder().startObject()
            .field("timestamp", "2023-06-01T12:00:00Z")
            .startArray("nested")
            .startObject()
            .startArray("nested1")
            .startObject()
            .field("status", 10)
            .endObject()
            .startObject()
            .field("status", 10)
            .endObject()
            .startObject()
            .field("name", "this is name")
            .endObject()
            .endArray()
            .endObject()
            .endArray()
            .endObject();
        // Index the document and refresh
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> client().prepareIndex(TEST_INDEX).setSource(doc).get()
        );
        assertEquals(
            "object mapping for [_doc] with array for [nested] cannot be accepted, as the field is also part of composite index mapping which does not accept arrays",
            ex.getMessage()
        );

        // Attempt to index a document with an array field
        final XContentBuilder doc1 = jsonBuilder().startObject()
            .field("timestamp", "2023-06-01T12:00:00Z")
            .startArray("nested")
            .startObject()
            .startArray("nested1")
            .startObject()
            .field("status", 10)
            .endObject()
            .startObject()
            .field("name", "this is name")
            .endObject()
            .startObject()
            .field("name", "this is name 1")
            .endObject()
            .endArray()
            .endObject()
            .endArray()
            .endObject();
        // Index the document and refresh
        MapperParsingException ex1 = expectThrows(
            MapperParsingException.class,
            () -> client().prepareIndex(TEST_INDEX).setSource(doc1).get()
        );
        assertEquals(
            "object mapping for [_doc] with array for [nested] cannot be accepted, as the field is also part of composite index mapping which does not accept arrays",
            ex1.getMessage()
        );
    }

    public void testCompositeIndexWithArraysInChildNestedCompositeField() throws IOException {
        prepareCreate(TEST_INDEX).setSettings(settings).setMapping(createNestedTestMapping()).get();
        // here nested.nested1.status is part of the composite field but "nested.nested1" field is an array
        XContentBuilder doc = jsonBuilder().startObject()
            .field("timestamp", "2023-06-01T12:00:00Z")
            .startObject("nested")
            .startArray("nested1")
            .startObject()
            .field("status", 10)
            .endObject()
            .startObject()
            .field("name", "this is name")
            .endObject()
            .startObject()
            .field("name", "this is name1")
            .endObject()
            .endArray()
            .endObject()
            .endObject();
        // Index the document and refresh
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> client().prepareIndex(TEST_INDEX).setSource(doc).get()
        );
        assertEquals(
            "object mapping for [nested] with array for [nested1] cannot be accepted, as the field is also part of composite index mapping which does not accept arrays",
            ex.getMessage()
        );
    }

    public void testCompositeIndexWithArraysInNestedMultiFields() throws IOException {
        prepareCreate(TEST_INDEX).setSettings(settings).setMapping(createNestedTestMapping()).get();
        // here nested.nested1.status is part of the composite field but "nested.nested1" field is an array
        XContentBuilder doc = jsonBuilder().startObject()
            .field("timestamp", "2023-06-01T12:00:00Z")
            .startObject("nested")
            .startObject("nested1")
            .field("status", 10)
            .startArray("name")
            .value("this is name")
            .value("this is name1")
            .endArray()
            .endObject()
            .endObject()
            .endObject();
        // Index the document and refresh
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> client().prepareIndex(TEST_INDEX).setSource(doc).get()
        );
        assertEquals(
            "object mapping for [nested.nested1] with array for [name] cannot be accepted, as the field is also part of composite index mapping which does not accept arrays",
            ex.getMessage()
        );
    }

    public void testCompositeIndexWithArraysInNestedCompositeFieldSameNameAsNormalField() throws IOException {
        prepareCreate(TEST_INDEX).setSettings(settings).setMapping(createNestedTestMappingForArray()).get();
        // here status is part of the composite field but "nested.nested1.status" field is an array which is not
        // part of composite field
        XContentBuilder doc = jsonBuilder().startObject()
            .field("timestamp", "2023-06-01T12:00:00Z")
            .startObject("nested")
            .startObject("nested1")
            .startArray("status")
            .value(10)
            .value(20)
            .value(30)
            .endArray()
            .endObject()
            .endObject()
            .field("status", "200")
            .endObject();
        // Index the document and refresh
        // Index the document and refresh
        IndexResponse indexResponse = client().prepareIndex(TEST_INDEX).setSource(doc).get();

        assertEquals(RestStatus.CREATED, indexResponse.status());

        client().admin().indices().prepareRefresh(TEST_INDEX).get();
        // Verify the document was indexed
        SearchResponse searchResponse = client().prepareSearch(TEST_INDEX).setQuery(QueryBuilders.matchAllQuery()).get();

        assertEquals(1, searchResponse.getHits().getTotalHits().value());

        // Verify the values in the indexed document
        SearchHit hit = searchResponse.getHits().getAt(0);
        assertEquals("2023-06-01T12:00:00Z", hit.getSourceAsMap().get("timestamp"));

        int values = Integer.parseInt((String) hit.getSourceAsMap().get("status"));
        assertEquals(200, values);
    }

    public void testCompositeIndexWithNestedArraysInNonCompositeField() throws IOException {
        prepareCreate(TEST_INDEX).setSettings(settings).setMapping(createNestedTestMapping()).get();
        // Attempt to index a document with an array field
        XContentBuilder doc = jsonBuilder().startObject()
            .field("timestamp", "2023-06-01T12:00:00Z")
            .startObject("nested-not-startree")
            .startArray("nested1")
            .startObject()
            .field("status", 10)
            .endObject()
            .startObject()
            .field("status", 20)
            .endObject()
            .startObject()
            .field("status", 30)
            .endObject()
            .endArray()
            .endObject()
            .endObject();

        // Index the document and refresh
        IndexResponse indexResponse = client().prepareIndex(TEST_INDEX).setSource(doc).get();

        assertEquals(RestStatus.CREATED, indexResponse.status());

        client().admin().indices().prepareRefresh(TEST_INDEX).get();
        // Verify the document was indexed
        SearchResponse searchResponse = client().prepareSearch(TEST_INDEX).setQuery(QueryBuilders.matchAllQuery()).get();

        assertEquals(1, searchResponse.getHits().getTotalHits().value());

        // Verify the values in the indexed document
        SearchHit hit = searchResponse.getHits().getAt(0);
        assertEquals("2023-06-01T12:00:00Z", hit.getSourceAsMap().get("timestamp"));

        List<Object> values = (List<Object>) ((Map<String, Object>) (hit.getSourceAsMap().get("nested-not-startree"))).get("nested1");
        assertEquals(3, values.size());
        int i = 1;
        for (Object val : values) {
            Map<String, Object> valMap = (Map<String, Object>) val;
            assertEquals(10 * i, valMap.get("status"));
            i++;
        }
    }

    public void testCompositeIndexWithArraysInNonCompositeField() throws IOException {
        prepareCreate(TEST_INDEX).setSettings(settings).setMapping(createMinimalTestMapping(false, false, false)).get();
        // Attempt to index a document with an array field
        XContentBuilder doc = jsonBuilder().startObject()
            .field("timestamp", "2023-06-01T12:00:00Z")
            .startArray("numeric")
            .value(10)
            .value(20)
            .value(30)
            .endArray()
            .endObject();

        // Index the document and refresh
        IndexResponse indexResponse = client().prepareIndex(TEST_INDEX).setSource(doc).get();

        assertEquals(RestStatus.CREATED, indexResponse.status());

        client().admin().indices().prepareRefresh(TEST_INDEX).get();
        // Verify the document was indexed
        SearchResponse searchResponse = client().prepareSearch(TEST_INDEX).setQuery(QueryBuilders.matchAllQuery()).get();

        assertEquals(1, searchResponse.getHits().getTotalHits().value());

        // Verify the values in the indexed document
        SearchHit hit = searchResponse.getHits().getAt(0);
        assertEquals("2023-06-01T12:00:00Z", hit.getSourceAsMap().get("timestamp"));

        List<Integer> values = (List<Integer>) hit.getSourceAsMap().get("numeric");
        assertEquals(3, values.size());
        assertTrue(values.contains(10));
        assertTrue(values.contains(20));
        assertTrue(values.contains(30));
    }

    public void testUpdateIndexWithAdditionOfStarTree() {
        prepareCreate(TEST_INDEX).setSettings(settings).setMapping(createMinimalTestMapping(false, false, false)).get();

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().preparePutMapping(TEST_INDEX).setSource(createUpdateTestMapping(false, false)).get()
        );
        assertEquals("Index cannot have more than [1] star tree fields", ex.getMessage());
    }

    public void testUpdateIndexWithNewerStarTree() {
        prepareCreate(TEST_INDEX).setSettings(settings).setMapping(createTestMappingWithoutStarTree()).get();

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().preparePutMapping(TEST_INDEX).setSource(createUpdateTestMapping(false, false)).get()
        );
        assertEquals(
            "Composite fields must be specified during index creation, addition of new composite fields during update is not supported",
            ex.getMessage()
        );
    }

    public void testUpdateIndexWhenMappingIsDifferent() {
        prepareCreate(TEST_INDEX).setSettings(settings).setMapping(createMinimalTestMapping(false, false, false)).get();

        // update some field in the mapping
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().preparePutMapping(TEST_INDEX).setSource(createUpdateTestMapping(true, true)).get()
        );
        assertTrue(ex.getMessage().contains("Cannot update parameter [config] from"));
    }

    public void testUpdateIndexWhenMappingIsSame() {
        prepareCreate(TEST_INDEX).setSettings(settings).setMapping(createMinimalTestMapping(false, false, false)).get();

        // update some field in the mapping
        AcknowledgedResponse putMappingResponse = client().admin()
            .indices()
            .preparePutMapping(TEST_INDEX)
            .setSource(createMinimalTestMapping(false, false, false))
            .get();
        assertAcked(putMappingResponse);

        Iterable<IndicesService> dataNodeInstances = internalCluster().getDataNodeInstances(IndicesService.class);
        for (IndicesService service : dataNodeInstances) {
            final Index index = resolveIndex("test");
            if (service.hasIndex(index)) {
                IndexService indexService = service.indexService(index);
                Set<CompositeMappedFieldType> fts = indexService.mapperService().getCompositeFieldTypes();

                for (CompositeMappedFieldType ft : fts) {
                    assertFalse(ft == null);
                    assertTrue(ft.unwrap() instanceof StarTreeMapper.StarTreeFieldType);
                    StarTreeMapper.StarTreeFieldType starTreeFieldType = (StarTreeMapper.StarTreeFieldType) ft;
                    assertEquals("timestamp", starTreeFieldType.getDimensions().get(0).getField());
                    assertTrue(starTreeFieldType.getDimensions().get(0) instanceof DateDimension);
                    DateDimension dateDim = (DateDimension) starTreeFieldType.getDimensions().get(0);
                    List<DateTimeUnitRounding> expectedTimeUnits = Arrays.asList(
                        new DateTimeUnitAdapter(Rounding.DateTimeUnit.MINUTES_OF_HOUR),
                        DataCubeDateTimeUnit.HALF_HOUR_OF_DAY
                    );
                    for (int i = 0; i < expectedTimeUnits.size(); i++) {
                        assertEquals(expectedTimeUnits.get(i).shortName(), dateDim.getIntervals().get(i).shortName());
                    }

                    assertEquals("numeric_dv", starTreeFieldType.getDimensions().get(1).getField());
                    assertEquals("numeric_dv", starTreeFieldType.getMetrics().get(0).getField());

                    // Assert default metrics
                    List<MetricStat> expectedMetrics = Arrays.asList(MetricStat.VALUE_COUNT, MetricStat.SUM, MetricStat.AVG);
                    assertEquals(expectedMetrics, starTreeFieldType.getMetrics().get(0).getMetrics());
                    assertEquals(10000, starTreeFieldType.getStarTreeConfig().maxLeafDocs());
                    assertEquals(
                        StarTreeFieldConfiguration.StarTreeBuildMode.OFF_HEAP,
                        starTreeFieldType.getStarTreeConfig().getBuildMode()
                    );
                    assertEquals(Collections.emptySet(), starTreeFieldType.getStarTreeConfig().getSkipStarNodeCreationInDims());
                }
            }
        }
    }

    public void testInvalidDimCompositeIndex() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> prepareCreate(TEST_INDEX).setSettings(settings).setMapping(createMinimalTestMapping(true, false, false)).get()
        );
        assertTrue(ex.getMessage().startsWith("Aggregations not supported for the dimension field "));
    }

    public void testMaxDimsCompositeIndex() {
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> prepareCreate(TEST_INDEX).setSettings(settings)
                .setMapping(createMaxDimTestMapping())
                // Date dimension is considered as one dimension regardless of number of actual calendar intervals
                .setSettings(
                    Settings.builder()
                        .put(StarTreeIndexSettings.STAR_TREE_MAX_DIMENSIONS_SETTING.getKey(), 2)
                        .put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true)
                        .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), true)
                        .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(512, ByteSizeUnit.MB))
                )
                .get()
        );
        assertEquals(
            "Failed to parse mapping [_doc]: ordered_dimensions cannot have more than 2 dimensions for star tree field [startree-1]",
            ex.getMessage()
        );
    }

    public void testMaxMetricsCompositeIndex() {
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> prepareCreate(TEST_INDEX).setSettings(settings)
                .setMapping(createMaxDimTestMapping())
                .setSettings(
                    Settings.builder()
                        .put(StarTreeIndexSettings.STAR_TREE_MAX_BASE_METRICS_SETTING.getKey(), 4)
                        .put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true)
                        .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), true)
                        .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(512, ByteSizeUnit.MB))
                )
                .get()
        );
        assertEquals(
            "Failed to parse mapping [_doc]: There cannot be more than [4] base metrics for star tree field [startree-1]",
            ex.getMessage()
        );
    }

    public void testMaxCalendarIntervalsCompositeIndex() {
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> prepareCreate(TEST_INDEX).setMapping(createMaxDimTestMapping())
                .setSettings(
                    Settings.builder()
                        .put(StarTreeIndexSettings.STAR_TREE_MAX_DATE_INTERVALS_SETTING.getKey(), 1)
                        .put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true)
                        .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), true)
                        .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(512, ByteSizeUnit.MB))
                )
                .get()
        );
        assertEquals(
            "Failed to parse mapping [_doc]: At most [1] calendar intervals are allowed in dimension [timestamp]",
            ex.getMessage()
        );
    }

    public void testUnsupportedDim() {
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> prepareCreate(TEST_INDEX).setSettings(settings).setMapping(createMinimalTestMapping(false, false, true)).get()
        );
        assertEquals(
            "Failed to parse mapping [_doc]: unsupported field type associated with dimension [wildcard] as part of star tree field [startree-1]",
            ex.getMessage()
        );
    }

    public void testInvalidMetric() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> prepareCreate(TEST_INDEX).setSettings(settings).setMapping(createMinimalTestMapping(false, true, false)).get()
        );
        assertEquals(
            "Aggregations not supported for the metrics field [numeric] with field type [integer] as part of star tree field",
            ex.getMessage()
        );
    }

    public void testDuplicateDimensions() {
        XContentBuilder finalMapping = getMappingWithDuplicateFields(true, false);
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> prepareCreate(TEST_INDEX).setSettings(settings).setMapping(finalMapping).setSettings(settings).get()
        );
        assertEquals(
            "Failed to parse mapping [_doc]: Duplicate dimension [numeric_dv] present as part star tree index field [startree-1]",
            ex.getMessage()
        );
    }

    public void testDuplicateMetrics() {
        XContentBuilder finalMapping = getMappingWithDuplicateFields(false, true);
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> prepareCreate(TEST_INDEX).setSettings(settings).setMapping(finalMapping).setSettings(settings).get()
        );
        assertEquals(
            "Failed to parse mapping [_doc]: Duplicate metrics [numeric_dv] present as part star tree index field [startree-1]",
            ex.getMessage()
        );
    }

    public void testValidTranslogFlushThresholdSize() {
        Settings indexSettings = Settings.builder()
            .put(settings)
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(256, ByteSizeUnit.MB))
            .build();

        AcknowledgedResponse response = prepareCreate(TEST_INDEX).setSettings(indexSettings)
            .setMapping(createMinimalTestMapping(false, false, false))
            .get();

        assertTrue(response.isAcknowledged());
    }

    public void testInvalidTranslogFlushThresholdSize() {
        Settings indexSettings = Settings.builder()
            .put(settings)
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(1024, ByteSizeUnit.MB))
            .build();

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> prepareCreate(TEST_INDEX).setSettings(indexSettings).setMapping(createMinimalTestMapping(false, false, false)).get()
        );

        assertTrue(
            ex.getMessage().contains("You can configure 'index.translog.flush_threshold_size' with upto '512mb' for composite index")
        );
    }

    public void testTranslogFlushThresholdSizeWithDefaultCompositeSettingLow() {
        Settings updatedSettings = Settings.builder()
            .put(CompositeIndexSettings.COMPOSITE_INDEX_MAX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), "130m")
            .build();

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest().transientSettings(updatedSettings);

        client().admin().cluster().updateSettings(updateSettingsRequest).actionGet();
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> prepareCreate(TEST_INDEX).setSettings(settings).setMapping(createMinimalTestMapping(false, false, false)).get()
        );

        assertEquals("You can configure 'index.translog.flush_threshold_size' with upto '130mb' for composite index", ex.getMessage());
    }

    public void testUpdateTranslogFlushThresholdSize() {
        prepareCreate(TEST_INDEX).setSettings(settings).setMapping(createMinimalTestMapping(false, false, false)).get();

        // Update to a valid value
        AcknowledgedResponse validUpdateResponse = client().admin()
            .indices()
            .prepareUpdateSettings(TEST_INDEX)
            .setSettings(Settings.builder().put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), "256mb"))
            .get();
        assertTrue(validUpdateResponse.isAcknowledged());

        // Try to update to an invalid value
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .indices()
                .prepareUpdateSettings(TEST_INDEX)
                .setSettings(Settings.builder().put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), "1024mb"))
                .get()
        );

        assertTrue(
            ex.getMessage().contains("You can configure 'index.translog.flush_threshold_size' with upto '512mb' for composite index")
        );

        // update cluster settings to higher value
        Settings updatedSettings = Settings.builder()
            .put(CompositeIndexSettings.COMPOSITE_INDEX_MAX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), "1030m")
            .build();

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest().transientSettings(updatedSettings);

        client().admin().cluster().updateSettings(updateSettingsRequest).actionGet();

        // update index threshold flush to higher value
        validUpdateResponse = client().admin()
            .indices()
            .prepareUpdateSettings(TEST_INDEX)
            .setSettings(Settings.builder().put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), "1024mb"))
            .get();
        assertTrue(validUpdateResponse.isAcknowledged());
    }

    public void testMinimumTranslogFlushThresholdSize() {
        Settings indexSettings = Settings.builder()
            .put(settings)
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(56, ByteSizeUnit.BYTES))
            .build();

        AcknowledgedResponse response = prepareCreate(TEST_INDEX).setSettings(indexSettings)
            .setMapping(createMinimalTestMapping(false, false, false))
            .get();

        assertTrue(response.isAcknowledged());
    }

    public void testBelowMinimumTranslogFlushThresholdSize() {
        Settings indexSettings = Settings.builder()
            .put(settings)
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(55, ByteSizeUnit.BYTES))
            .build();

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> prepareCreate(TEST_INDEX).setSettings(indexSettings).setMapping(createMinimalTestMapping(false, false, false)).get()
        );

        assertEquals("failed to parse value [55b] for setting [index.translog.flush_threshold_size], must be >= [56b]", ex.getMessage());
    }

    @After
    public final void cleanupNodeSettings() {
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putNull("*"))
                .setTransientSettings(Settings.builder().putNull("*"))
        );
    }
}
