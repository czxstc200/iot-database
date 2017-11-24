/**
 * Copyright © 2016-2017 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.dao.timeseries;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.kv.TsKvEntry;
import org.thingsboard.server.common.data.kv.TsKvQuery;
import org.thingsboard.server.common.data.rule.RuleMetaData;
import org.thingsboard.server.dao.exception.IncorrectParameterException;
import org.thingsboard.server.dao.rule.RuleService;
import org.thingsboard.server.dao.service.Validator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.dao.device.DeviceService;
import org.thingsboard.server.common.data.*;

import java.util.*;

import java.io.IOException;

import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * @author Andrew Shvayka
 */
@Service
@Slf4j
public class BaseTimeseriesService implements TimeseriesService {

    public static final int INSERTS_PER_ENTRY = 3;

    @Autowired
    private TimeseriesDao timeseriesDao;

    @Autowired
    private DeviceService deviceService;

    @Autowired
    private RuleService ruleService;

    @Override
    public ListenableFuture<List<TsKvEntry>> findAll(EntityId entityId, List<TsKvQuery> queries) {
        validate(entityId);
        queries.forEach(query -> validate(query));
        return timeseriesDao.findAllAsync(entityId, queries);
    }

    @Override
    public ListenableFuture<List<TsKvEntry>> findLatest(EntityId entityId, Collection<String> keys) {
        validate(entityId);
        List<ListenableFuture<TsKvEntry>> futures = Lists.newArrayListWithExpectedSize(keys.size());
        keys.forEach(key -> Validator.validateString(key, "Incorrect key " + key));
        keys.forEach(key -> futures.add(timeseriesDao.findLatest(entityId, key)));
        return Futures.allAsList(futures);
    }

    @Override
    public ListenableFuture<List<TsKvEntry>> findAllLatest(EntityId entityId) {
        validate(entityId);
        return timeseriesDao.findAllLatest(entityId);
    }

    @Override
    public ListenableFuture<List<Void>> save(EntityId entityId, TsKvEntry tsKvEntry) {
        validate(entityId);
        if (tsKvEntry == null) {
            throw new IncorrectParameterException("Key value entry can't be null");
        }
        List<ListenableFuture<Void>> futures = Lists.newArrayListWithExpectedSize(INSERTS_PER_ENTRY);
        saveAndRegisterFutures(futures, entityId, tsKvEntry, 0L);
        return Futures.allAsList(futures);
    }

    @Override
    public ListenableFuture<List<Void>> save(EntityId entityId, List<TsKvEntry> tsKvEntries, long ttl) {
        List<ListenableFuture<Void>> futures = Lists.newArrayListWithExpectedSize(tsKvEntries.size() * INSERTS_PER_ENTRY);
        Map<String,Object> sensors = new HashMap<String,Object>();
        for (TsKvEntry tsKvEntry : tsKvEntries) {
            /*Optional<RuleMetaData> rule = ruleService.findRuleBySensorId(UUID.fromString("38400000-8cf0-11bd-b23e-10b96e4ef00d"));//这个UUID作为测试 实际应从tskventry获取
            if (rule.isPresent() && tsKvEntry.getKey().equals("temperature")) {
                Double lower_limit=rule.get().getLower_limit();
                Double upper_limit=rule.get().getUpper_limit();
                if((tsKvEntry.getDoubleValue().isPresent())){
                    //String ttt = tsKvEntry.getValue().toString();
                    if(lower_limit>tsKvEntry.getDoubleValue().get()||tsKvEntry.getDoubleValue().get()>upper_limit){//异常，需要报警
                        rule.get().setAlarm_status(true);
                    }
                    else{
                        rule.get().setAlarm_status(false);
                    }
                    ruleService.saveRule(rule.get());
                }

            }*/
            if (tsKvEntry == null) {
                throw new IncorrectParameterException("Key value entry can't be null");
            }
            saveAndRegisterFutures(futures, entityId, tsKvEntry, ttl);

            Map<String,Object> sensor = new HashMap<String,Object>();
            sensor.put("sensor_name",tsKvEntry.getKey());
            if (tsKvEntry.getBooleanValue().isPresent()) {
                sensor.put("sensor_type","BOOLEAN");
                sensor.put("sensor_value",tsKvEntry.getBooleanValue().get());
            }else if(tsKvEntry.getStrValue().isPresent()){
                sensor.put("sensor_type","STRING");
                sensor.put("sensor_value",tsKvEntry.getStrValue().get());
            }else if(tsKvEntry.getLongValue().isPresent()){
                sensor.put("sensor_type","NUMERICAL");
                sensor.put("sensor_value",tsKvEntry.getLongValue().get());
            }else if(tsKvEntry.getDoubleValue().isPresent()){
                sensor.put("sensor_type","NUMERICAL");
                sensor.put("sensor_value",tsKvEntry.getDoubleValue().get());
            }
            sensors.put(tsKvEntry.getKey(),sensor);
        }
        DeviceId deviceId = new DeviceId(entityId.getId());
        Device device = deviceService.findDeviceById(deviceId);
       // String json = "{\"sensor_name\":\"temperature\",\"sensor_ID\":\"temp_1\",\"type\":\"On_Off\",\"decimal\":\"0.2\",\"unit\":\"C\",\"location\":{\"longitude\":100,\"latitude\":100,\"height\":15},\"value\":\"15\"}";
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode node = objectMapper.valueToTree(sensors);
        device.setSensor(node);
        deviceService.saveDevice(device);

        /*for (RuleMetaData rule : rules){
            Double lower_limit=rule.getLower_limit();
            Double upper_limit=rule.getUpper_limit();
        }*/

        return Futures.allAsList(futures);
    }

    private void saveAndRegisterFutures(List<ListenableFuture<Void>> futures, EntityId entityId, TsKvEntry tsKvEntry, long ttl) {
        futures.add(timeseriesDao.savePartition(entityId, tsKvEntry.getTs(), tsKvEntry.getKey(), ttl));
        futures.add(timeseriesDao.saveLatest(entityId, tsKvEntry));
        futures.add(timeseriesDao.save(entityId, tsKvEntry, ttl));
    }

    private static void validate(EntityId entityId) {
        Validator.validateEntityId(entityId, "Incorrect entityId " + entityId);
    }

    private static void validate(TsKvQuery query) {
        if (query == null) {
            throw new IncorrectParameterException("TsKvQuery can't be null");
        } else if (isBlank(query.getKey())) {
            throw new IncorrectParameterException("Incorrect TsKvQuery. Key can't be empty");
        } else if (query.getAggregation() == null) {
            throw new IncorrectParameterException("Incorrect TsKvQuery. Aggregation can't be empty");
        }
    }
}
