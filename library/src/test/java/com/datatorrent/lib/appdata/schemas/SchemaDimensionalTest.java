/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datatorrent.lib.appdata.schemas;

import com.datatorrent.lib.appdata.dimensions.AggregatorUtils;
import com.datatorrent.lib.appdata.qr.DataSerializerFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SchemaDimensionalTest
{
  private static final Logger logger = LoggerFactory.getLogger(SchemaDimensionalTest.class);

  public SchemaDimensionalTest()
  {
  }

  @Before
  public void initialize()
  {
    AggregatorUtils.DEFAULT_AGGREGATOR_INFO.setup();
  }

  @Test
  public void globalValueTest() throws Exception
  {
    String resultSchema = produceSchema("adsGenericEventSchema.json");

    List<String> timeBuckets = Lists.newArrayList("1m", "1h", "1d");
    List<String> keyNames = Lists.newArrayList("publisher", "advertiser", "location");
    List<String> keyTypes = Lists.newArrayList("string", "string", "string");

    Map<String, String> valueToType = Maps.newHashMap();
    valueToType.put("impressions:SUM", "long");
    valueToType.put("clicks:SUM", "long");
    valueToType.put("cost:SUM", "double");
    valueToType.put("revenue:SUM", "double");

    @SuppressWarnings("unchecked")
    List<Set<String>> dimensionCombinationsList = Lists.newArrayList((Set<String>) new HashSet<String>(),
                                                                     Sets.newHashSet("location"),
                                                                     Sets.newHashSet("advertiser"),
                                                                     Sets.newHashSet("publisher"),
                                                                     Sets.newHashSet("location", "advertiser"),
                                                                     Sets.newHashSet("location", "publisher"),
                                                                     Sets.newHashSet("advertiser", "publisher"),
                                                                     Sets.newHashSet("location", "advertiser", "publisher"));

    basicSchemaChecker(resultSchema,
                       timeBuckets,
                       keyNames,
                       keyTypes,
                       valueToType,
                       dimensionCombinationsList);
  }

  @Test
  public void additionalValueTest() throws Exception
  {
    String resultSchema = produceSchema("adsGenericEventSchemaAdditional.json");

    List<String> timeBuckets = Lists.newArrayList("1m", "1h", "1d");
    List<String> keyNames = Lists.newArrayList("publisher", "advertiser", "location");
    List<String> keyTypes = Lists.newArrayList("string", "string", "string");

    Map<String, String> valueToType = Maps.newHashMap();
    valueToType.put("impressions:SUM", "long");
    valueToType.put("impressions:COUNT", "long");
    valueToType.put("clicks:SUM", "long");
    valueToType.put("clicks:COUNT", "long");
    valueToType.put("cost:SUM", "double");
    valueToType.put("cost:COUNT", "long");
    valueToType.put("revenue:SUM", "double");
    valueToType.put("revenue:COUNT", "long");

    @SuppressWarnings("unchecked")
    List<Set<String>> dimensionCombinationsList = Lists.newArrayList((Set<String>) new HashSet<String>(),
                                                                     Sets.newHashSet("location"),
                                                                     Sets.newHashSet("advertiser"),
                                                                     Sets.newHashSet("publisher"),
                                                                     Sets.newHashSet("location", "advertiser"),
                                                                     Sets.newHashSet("location", "publisher"),
                                                                     Sets.newHashSet("advertiser", "publisher"),
                                                                     Sets.newHashSet("location", "advertiser", "publisher"));

    basicSchemaChecker(resultSchema,
                       timeBuckets,
                       keyNames,
                       keyTypes,
                       valueToType,
                       dimensionCombinationsList);

    Map<String, String> additionalValueMap = Maps.newHashMap();
    additionalValueMap.put("impressions:MAX", "long");
    additionalValueMap.put("clicks:MAX", "long");
    additionalValueMap.put("cost:MAX", "double");
    additionalValueMap.put("revenue:MAX", "double");

    @SuppressWarnings("unchecked")
    List<Map<String, String>> additionalValuesList = Lists.newArrayList((Map<String, String>) new HashMap<String, String>(),
                                                                (Map<String, String>) new HashMap<String, String>(),
                                                                additionalValueMap,
                                                                additionalValueMap,
                                                                (Map<String, String>) new HashMap<String, String>(),
                                                                (Map<String, String>) new HashMap<String, String>(),
                                                                (Map<String, String>) new HashMap<String, String>(),
                                                                (Map<String, String>) new HashMap<String, String>());

    JSONObject data = new JSONObject(resultSchema).getJSONArray("data").getJSONObject(0);
    JSONArray dimensions = data.getJSONArray("dimensions");

    for(int index = 0;
        index < dimensions.length();
        index++) {
      JSONObject combination = dimensions.getJSONObject(index);

      Map<String, String> tempAdditionalValueMap = additionalValuesList.get(index);
      Assert.assertEquals(tempAdditionalValueMap.isEmpty(), !combination.has("additionalValues"));

      Set<String> additionalValueSet = Sets.newHashSet();

      if(tempAdditionalValueMap.isEmpty()) {
        continue;
      }

      JSONArray additionalValues = combination.getJSONArray("additionalValues");

      for(int aIndex = 0;
          aIndex < additionalValues.length();
          aIndex++) {
        JSONObject additionalValue = additionalValues.getJSONObject(aIndex);

        String valueName = additionalValue.getString("name");
        String valueType = additionalValue.getString("type");

        String expectedValueType = tempAdditionalValueMap.get(valueName);

        Assert.assertTrue("Duplicate value " + valueName, additionalValueSet.add(valueName));
        Assert.assertTrue("Invalid value " + valueName, expectedValueType != null);
        Assert.assertEquals(expectedValueType, valueType);
      }
    }
  }

  private String produceSchema(String resourceName) throws Exception
  {
    String eventSchemaJSON = SchemaUtils.jarResourceFileToString(resourceName);

    DataSerializerFactory dsf = new DataSerializerFactory(new AppDataFormatter());
    SchemaDimensional schemaDimensional = new SchemaDimensional(new DimensionalEventSchema(eventSchemaJSON,
                                                                                           AggregatorUtils.DEFAULT_AGGREGATOR_INFO));

    SchemaQuery schemaQuery = new SchemaQuery();
    schemaQuery.setId("1");
    schemaQuery.setType(SchemaQuery.TYPE);

    SchemaResult result = new SchemaResult(schemaQuery, schemaDimensional);
    String resultJSON = dsf.serialize(result);
    return dsf.serialize(result);
  }

  private void basicSchemaChecker(String resultSchema,
                                  List<String> timeBuckets,
                                  List<String> keyNames,
                                  List<String> keyTypes,
                                  Map<String, String> valueToType,
                                  List<Set<String>> dimensionCombinationsList) throws Exception
  {
    JSONObject schemaJO = new JSONObject(resultSchema);
    JSONObject data = schemaJO.getJSONArray("data").getJSONObject(0);

    JSONArray jaBuckets = SchemaUtils.findFirstKeyJSONArray(schemaJO, "buckets");

    Assert.assertEquals(timeBuckets.size(), jaBuckets.length());

    for(int index = 0;
        index < jaBuckets.length();
        index++) {
      Assert.assertEquals(timeBuckets.get(index), jaBuckets.get(index));
    }

    JSONArray keys = data.getJSONArray("keys");

    for(int index = 0;
        index < keys.length();
        index++) {
      JSONObject keyJO = keys.getJSONObject(index);

      Assert.assertEquals(keyNames.get(index), keyJO.get("name"));
      Assert.assertEquals(keyTypes.get(index), keyJO.get("type"));
      Assert.assertTrue(keyJO.has("enumValues"));
    }

    JSONArray valuesArray = data.getJSONArray("values");

    Assert.assertEquals("Incorrect number of values.", valueToType.size(), valuesArray.length());

    Set<String> valueNames = Sets.newHashSet();

    for(int index = 0;
        index < valuesArray.length();
        index++) {
      JSONObject valueJO = valuesArray.getJSONObject(index);

      String valueName = valueJO.getString("name");
      String typeName = valueJO.getString("type");

      String expectedType = valueToType.get(valueName);

      Assert.assertTrue("Duplicate value name " + valueName, valueNames.add(valueName));
      Assert.assertTrue("Invalid value name " + valueName, expectedType != null);

      Assert.assertEquals(expectedType, typeName);
    }

    JSONArray dimensions = data.getJSONArray("dimensions");

    for(int index = 0;
        index < dimensions.length();
        index++) {
      JSONObject combination = dimensions.getJSONObject(index);
      JSONArray dimensionsCombinationArray = combination.getJSONArray("combination");

      Set<String> dimensionCombination = Sets.newHashSet();

      for(int dimensionIndex = 0;
          dimensionIndex < dimensionsCombinationArray.length();
          dimensionIndex++) {
        dimensionCombination.add(dimensionsCombinationArray.getString(dimensionIndex));
      }

      Assert.assertEquals(dimensionCombinationsList.get(index), dimensionCombination);
    }
  }
}