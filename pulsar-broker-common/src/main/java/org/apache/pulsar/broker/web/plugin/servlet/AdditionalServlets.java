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
package org.apache.pulsar.broker.web.plugin.servlet;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.configuration.PulsarConfiguration;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.plugin.PluginLoader;

/**
 * A collection of loaded additional servlets.
 */
@Slf4j
public class AdditionalServlets implements AutoCloseable {

    private static final String ADDITIONAL_SERVLET_FILE = "additional_servlet.yml";

    private static final String ADDITIONAL_SERVLET_DIRECTORY = "additionalServletDirectory";

    private static final String ADDITIONAL_SERVLETS = "additionalServlets";

    private static final String NAR_EXTRACTION_DIRECTORY = "narExtractionDirectory";

    @Deprecated
    private static final String PROXY_ADDITIONAL_SERVLET_DIRECTORY = "proxyAdditionalServletDirectory";

    @Deprecated
    private static final String PROXY_ADDITIONAL_SERVLETS = "proxyAdditionalServlets";

    @Getter
    private final Map<String, AdditionalServletWithClassLoader> servlets;

    public AdditionalServlets(Map<String, AdditionalServletWithClassLoader> servlets) {
        this.servlets = servlets;
    }

    /**
     * Load the additional servlet for the given <tt>servlet name</tt> list.
     *
     * @param conf the pulsar service configuration
     * @return the collection of additional servlet
     */
    public static AdditionalServlets load(PulsarConfiguration conf) throws IOException {
        String additionalServletDirectory = conf.getProperties().getProperty(ADDITIONAL_SERVLET_DIRECTORY);
        if (additionalServletDirectory == null) {
            // Compatible with the current proxy configuration
            additionalServletDirectory = conf.getProperties().getProperty(PROXY_ADDITIONAL_SERVLET_DIRECTORY);
        }

        String additionalServlets = conf.getProperties().getProperty(ADDITIONAL_SERVLETS);
        if (additionalServlets == null) {
            additionalServlets = conf.getProperties().getProperty(PROXY_ADDITIONAL_SERVLETS);
        }

        String narExtractionDirectory = conf.getProperties().getProperty(NAR_EXTRACTION_DIRECTORY);
        if (StringUtils.isBlank(narExtractionDirectory)) {
            narExtractionDirectory = NarClassLoader.DEFAULT_NAR_EXTRACTION_DIR;
        }

        if (additionalServletDirectory == null || additionalServlets == null) {
            return null;
        }

        Map<String, AdditionalServletWithClassLoader> servlets = new PluginLoader<>(
                "additional servlet",
                AdditionalServlet.class,
                AdditionalServletDefinition.class,
                AdditionalServletWithClassLoader::new,
                additionalServletDirectory,
                narExtractionDirectory,
                Arrays.asList(additionalServlets.split(",")),
                ADDITIONAL_SERVLET_FILE).load();
        if (servlets != null && !servlets.isEmpty()) {
            return new AdditionalServlets(servlets);
        }

        return null;
    }

    @Override
    public void close() {
        servlets.values().forEach(AdditionalServletWithClassLoader::close);
    }
}
