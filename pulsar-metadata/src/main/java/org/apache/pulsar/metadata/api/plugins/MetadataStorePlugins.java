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
package org.apache.pulsar.metadata.api.plugins;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.pulsar.common.plugin.PluginLoader;
import org.apache.pulsar.common.plugin.PluginMetadata;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;

public class MetadataStorePlugins extends
        PluginLoader<MetadataStorePlugin, MetadataStoreDefinition, MetadataStorePluginWithClassLoader>{

    private final MetadataStoreConfig config;

    private Map<String, PluginMetadata<MetadataStoreDefinition>> definitions;

    public MetadataStorePlugins(String metadataURL, MetadataStoreConfig config) {
        super("metadata store",
                MetadataStorePlugin.class,
                MetadataStoreDefinition.class,
                MetadataStorePluginWithClassLoader::new,
                config.getPluginsDirectory(),
                config.getNarExtractionDirectory(),
                Collections.singleton(getScheme(metadataURL)),
                "metadata-store.yml");
        this.config = config;
    }

    public boolean canHandle(String metadataURL) throws MetadataStoreException {
        if (definitions == null) {
            loadDefinitions();
        }
        String scheme = getScheme(metadataURL);
        return scheme != null && definitions.values().stream()
                .anyMatch(def -> def.getDefinition().getName().equals(scheme));
    }

    private static String getScheme(String metadataURL) {
        String[] split = metadataURL.split("://", 2);
        if (split.length > 1) {
            return split[0];
        }
        return null;
    }

    private void loadDefinitions() throws MetadataStoreException{
        try {
            definitions = this.searchForPluginDefinitions(config.getNarExtractionDirectory());
        } catch (IOException e) {
            throw new MetadataStoreException.InvalidImplementationException(e);
        }
    }

    public MetadataStore load(
            String metadataURL,
            MetadataStoreConfig metadataStoreConfig,
            boolean enableSessionWatcher) throws MetadataStoreException {
        try {
            String scheme = getScheme(metadataURL);
            ImmutableMap<String, MetadataStorePluginWithClassLoader> plugins = load();
            MetadataStorePluginWithClassLoader plugin = plugins.get(scheme);
            if (plugin == null) {
                throw new MetadataStoreException.InvalidImplementationException(
                        "Error retrieving plugin implementation");
            }
            return plugin.create(metadataURL, metadataStoreConfig, enableSessionWatcher);
        } catch (IOException e) {
            throw new MetadataStoreException.InvalidImplementationException(e);
        }
    }

}
