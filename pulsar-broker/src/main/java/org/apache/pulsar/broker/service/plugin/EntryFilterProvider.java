/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.service.plugin;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.plugin.PluginLoader;
import org.apache.pulsar.common.policies.data.EntryFilters;

@Slf4j
public class EntryFilterProvider extends PluginLoader<EntryFilter, EntryFilterDefinition, EntryFilterWithClassLoader> {

    @VisibleForTesting
    static final String ENTRY_FILTER_DEFINITION_FILE = "entry_filter";

    protected EntryFilterProvider(ServiceConfiguration conf,  Collection<String> entryFilterNames) {
        super("entry filter",
                EntryFilter.class,
                EntryFilterDefinition.class,
                EntryFilterWithClassLoader::new,
                conf.getEntryFiltersDirectory(),
                conf.getNarExtractionDirectory(),
                entryFilterNames,
                ENTRY_FILTER_DEFINITION_FILE + ".yaml",
                ENTRY_FILTER_DEFINITION_FILE + ".yml");
    }

    /**
     * create entry filter instance.
     */
    public static ImmutableMap<String, EntryFilterWithClassLoader> createEntryFilters(ServiceConfiguration conf,
                                                                                      EntryFilters entryFilters)
            throws IOException {
        String filterNames = entryFilters.getEntryFilterNames();
        Collection<String> namesSplit = Arrays.asList(filterNames.split(","));
        return new EntryFilterProvider(conf, namesSplit).load();
    }

    public static ImmutableMap<String, EntryFilterWithClassLoader> createEntryFilters(
            ServiceConfiguration conf) throws IOException {

        return new EntryFilterProvider(conf, conf.getEntryFilterNames()).load();
    }

}
