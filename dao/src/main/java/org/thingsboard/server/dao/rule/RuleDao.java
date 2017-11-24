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
package org.thingsboard.server.dao.rule;

import org.thingsboard.server.common.data.id.RuleId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.page.TextPageLink;
import org.thingsboard.server.common.data.rule.RuleMetaData;
import org.thingsboard.server.dao.Dao;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface RuleDao extends Dao<RuleMetaData> {

    RuleMetaData save(RuleMetaData rule);

    RuleMetaData findById(RuleId ruleId);

    List<RuleMetaData> findRulesByPlugin(String pluginToken);

    List<RuleMetaData> findRulesByDeviceId(String device_id);

    Optional<RuleMetaData> findRuleBySensorId(UUID sensor_id);

    List<RuleMetaData> findByTenantIdAndPageLink(TenantId tenantId, TextPageLink pageLink);

    /**
     * Find all tenant rules (including system) by tenantId and page link.
     *
     * @param tenantId the tenantId
     * @param pageLink the page link
     * @return the list of rules objects
     */
    List<RuleMetaData> findAllTenantRulesByTenantId(UUID tenantId, TextPageLink pageLink);

    void deleteById(UUID id);

    void deleteById(RuleId ruleId);
}
