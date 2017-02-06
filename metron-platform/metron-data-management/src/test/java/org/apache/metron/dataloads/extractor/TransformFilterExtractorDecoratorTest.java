package org.apache.metron.dataloads.extractor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.adrianwalker.multilinestring.Multiline;
import org.apache.curator.framework.CuratorFramework;
import org.apache.metron.enrichment.converter.EnrichmentKey;
import org.apache.metron.enrichment.converter.EnrichmentValue;
import org.apache.metron.enrichment.lookup.LookupKV;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.*;

public class TransformFilterExtractorDecoratorTest {

  @Mock
  CuratorFramework zkClient;
  @Mock
  Extractor extractor;
  LinkedHashMap<String, Object> config1;
  TransformFilterExtractorDecorator decorator;

  @Before
  public void setup() throws Exception {
    MockitoAnnotations.initMocks(this);
    config1 = new ObjectMapper().readValue(config1Contents, LinkedHashMap.class);
    decorator = new TransformFilterExtractorDecorator(extractor);
    decorator.setZkClient(Optional.of(zkClient));
    decorator.initialize(config1);
  }

  /**
   *{
   *  "zk_quorum" : "blah",
   *  "columns" : {
   *    "foo" : 0,
   *    "bar" : 1,
   *    "baz" : 2
   *  },
   *  "value_transform" : {
   *    "foo" : "TO_UPPER(foo)",
   *    "newvar" : "foo",
   *    "lowernewvar" : "TO_LOWER(newvar)"
   *  },
   *  "value_filter" : "LENGTH(baz) > 0",
   *  "indicator_column" : "bar",
   *  "indicator_transform" : {
   *    "somevar" : "indicator",
   *    "indicator" : "TO_UPPER(somevar)"
   *  },
   *  "indicator_filter" : "LENGTH(indicator) > 0",
   *  "type" : "testenrichment",
   *  "separator" : ","
   *}
   */
  @Multiline
  public static String config1Contents;

  @Test
  public void transforms_values_and_indicators() throws IOException {
    final String indicatorVal = "val2";
    EnrichmentKey lookupKey = new EnrichmentKey("testenrichment", indicatorVal);
    EnrichmentValue lookupValue = new EnrichmentValue(new HashMap<String, Object>() {{
      put("foo", "val1");
      put("bar", indicatorVal);
      put("baz", "val3");
    }});
    LookupKV lkv = new LookupKV<>(lookupKey, lookupValue);
    List<LookupKV> extractedLkvs = new ArrayList<>();
    extractedLkvs.add(lkv);
    Mockito.when(extractor.extract("val1,val2,val3")).thenReturn(extractedLkvs);
    Iterable<LookupKV> extracted = decorator.extract("val1,val2,val3");

    EnrichmentKey expectedLookupKey = new EnrichmentKey("testenrichment", "VAL2");
    EnrichmentValue expectedLookupValue = new EnrichmentValue(new HashMap<String, Object>() {{
      put("foo", "VAL1");
      put("bar", "val2");
      put("baz", "val3");
      put("newvar", "VAL1");
      put("lowernewvar", "val1");
    }});
    LookupKV expectedLkv = new LookupKV<>(expectedLookupKey, expectedLookupValue);
    List<LookupKV> expectedLkvs = new ArrayList<>();
    expectedLkvs.add(expectedLkv);
    Assert.assertThat(extracted, CoreMatchers.equalTo(expectedLkvs));
  }

  @Test
  public void filters_values() throws Exception {
    final String indicatorVal = "val2";
    EnrichmentKey lookupKey = new EnrichmentKey("testenrichment", indicatorVal);
    EnrichmentValue lookupValue = new EnrichmentValue(new HashMap<String, Object>() {{
      put("foo", "val1");
      put("bar", indicatorVal);
      put("baz", "");
    }});
    LookupKV lkv = new LookupKV<>(lookupKey, lookupValue);
    List<LookupKV> extractedLkvs = new ArrayList<>();
    extractedLkvs.add(lkv);
    Mockito.when(extractor.extract("val1,val2,")).thenReturn(extractedLkvs);
    Iterable<LookupKV> extracted = decorator.extract("val1,val2,");
    Assert.assertThat(extracted, CoreMatchers.equalTo(new ArrayList<>()));
  }

  @Test
  public void filters_indicators() throws Exception {
    EnrichmentKey lookupKey = new EnrichmentKey("testenrichment", "");
    EnrichmentValue lookupValue = new EnrichmentValue(new HashMap<String, Object>() {{
      put("foo", "val1");
      put("bar", "");
      put("baz", "val3");
    }});
    LookupKV lkv = new LookupKV<>(lookupKey, lookupValue);
    List<LookupKV> extractedLkvs = new ArrayList<>();
    extractedLkvs.add(lkv);
    Mockito.when(extractor.extract("val1,,val3")).thenReturn(extractedLkvs);
    Iterable<LookupKV> extracted = decorator.extract("val1,,val3");
    Assert.assertThat(extracted, CoreMatchers.equalTo(new ArrayList<>()));
  }

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void bad_value_transform_causes_exception() throws Exception {
    final int badValue = 5;
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Object is not of type java.util.Map");
    config1.put(TransformFilterExtractorDecorator.ExtractorOptions.VALUE_TRANSFORM.toString(), badValue);
    decorator = new TransformFilterExtractorDecorator(extractor);
    decorator.setZkClient(Optional.of(zkClient));
    decorator.initialize(config1);
  }

  @Test
  public void bad_value_filter_causes_exception() throws Exception {
    final int badValue = 5;
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Object is not of type java.lang.String");
    config1.put(TransformFilterExtractorDecorator.ExtractorOptions.VALUE_FILTER.toString(), badValue);
    decorator = new TransformFilterExtractorDecorator(extractor);
    decorator.setZkClient(Optional.of(zkClient));
    decorator.initialize(config1);
  }

  @Test
  public void bad_indicator_transform_causes_exception() throws Exception {
    final int badValue = 5;
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Object is not of type java.util.Map");
    config1.put(TransformFilterExtractorDecorator.ExtractorOptions.INDICATOR_TRANSFORM.toString(), badValue);
    decorator = new TransformFilterExtractorDecorator(extractor);
    decorator.setZkClient(Optional.of(zkClient));
    decorator.initialize(config1);
  }

  @Test
  public void bad_indicator_filter_causes_exception() throws Exception {
    final int badValue = 5;
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Object is not of type java.lang.String");
    config1.put(TransformFilterExtractorDecorator.ExtractorOptions.INDICATOR_FILTER.toString(), badValue);
    decorator = new TransformFilterExtractorDecorator(extractor);
    decorator.setZkClient(Optional.of(zkClient));
    decorator.initialize(config1);
  }

}