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
package org.apache.pulsar.common.plugin;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Map;
import java.util.function.BiFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.nar.NarClassLoaderBuilder;
import org.apache.pulsar.common.util.ObjectMapperFactory;

@Slf4j
public class PluginLoader<Plugin,
        Definition extends PluginDefinition,
        PluginWithClassLoader> {


    private String pluginType;
    private final Class<Plugin> pluginClass;
    private final Class<Definition> pluginDefinitionClass;
    private final BiFunction<Plugin, NarClassLoader, PluginWithClassLoader> wrapper;
    private final String pluginsDirectory;
    private final String narExtractionDirectory;
    private final Collection<String> pluginNames;
    private final String[] descriptorFileNames;

    private NarClassLoaderFactory classLoaderFactory;


    public PluginLoader(String pluginType,
                        Class<Plugin> pluginClass,
                        Class<Definition> pluginDefinitionClass,
                        BiFunction<Plugin, NarClassLoader, PluginWithClassLoader> wrapper,
                        String pluginsDirectory,
                        String narExtractionDirectory,
                        Collection<String> pluginNames,
                        String... descriptorFileNames) {
        this.pluginType = pluginType;
        this.pluginDefinitionClass = pluginDefinitionClass;
        this.pluginClass = pluginClass;
        this.wrapper = wrapper;
        this.pluginsDirectory = pluginsDirectory;
        this.narExtractionDirectory = narExtractionDirectory;
        this.pluginNames = pluginNames;
        this.descriptorFileNames = descriptorFileNames;
        this.classLoaderFactory = this::getNarClassLoader;
    }

    @VisibleForTesting
    void setClassLoaderFactory(NarClassLoaderFactory classLoaderFactory) {
        this.classLoaderFactory = classLoaderFactory;
    }

    protected void doubleCheck(String name, PluginWithClassLoader instance) {

    }


    public ImmutableMap<String, PluginWithClassLoader> load() throws IOException {
        Map<String, PluginMetadata<Definition>> definitions = searchForPluginDefinitions(narExtractionDirectory);

        ImmutableMap.Builder<String, PluginWithClassLoader> builder = ImmutableMap.builder();

        pluginNames.forEach(name -> {

            PluginMetadata<Definition> metadata = definitions.get(name);
            if (null == metadata) {
                throw new RuntimeException("No " + pluginType + " is found for name `" + name
                        + "`. Available plugins are : " + definitions);
            }

            PluginWithClassLoader instance;
            try {
                instance = load(metadata, narExtractionDirectory);

                doubleCheck(name, instance);

                builder.put(name, instance);
                log.info("Successfully loaded " + pluginType + " for `{}`", name);
            } catch (IOException e) {
                log.error("Failed to load the " + pluginType + " instance for name `" + name + "`", e);
                throw new RuntimeException("Failed to load the " + pluginType + " instance for name `" + name + "`");
            }
        });
        return builder.build();
    }



    /**
     * Retrieve the plugin definition from the provided nar package.
     *
     * @param narPath the path to the NAR package
     * @return the plugin definition
     * @throws IOException when failed to load the plugin package or get the definition
     */
    protected Definition getPluginDefinition(String narPath, String narExtractionDirectory)
            throws IOException {
        try (NarClassLoader ncl = this.classLoaderFactory.createLoader(narPath, narExtractionDirectory)) {
            return getPluginDefinition(ncl);
        }
    }

    private NarClassLoader getNarClassLoader(String narPath, String narExtractionDirectory) throws IOException {
        return NarClassLoaderBuilder.builder()
                .narFile(new File(narPath))
                .extractionDirectory(narExtractionDirectory)
                .build();
    }

    protected Definition getPluginDefinition(NarClassLoader ncl) throws IOException {

        String configStr = null;
        NoSuchFileException lastException = null;
        for (String file : descriptorFileNames) {
            try {
                configStr = ncl.getServiceDefinition(file);
                lastException = null;
                break;
            } catch (NoSuchFileException e) {
                lastException = e;
            }
        }
        if (lastException != null) {
            throw lastException;
        }

       return ObjectMapperFactory.getThreadLocalYaml().readValue(configStr, pluginDefinitionClass);
    }

    /**
     * Search and load the available plugins.
     *
     * @return a collection of plugin definitions
     * @throws IOException when failing to load the available plugins from the provided directory.
     */
    protected Map<String, PluginMetadata<Definition>> searchForPluginDefinitions(String narExtractionDirectory)
            throws IOException {
        Path path = Paths.get(pluginsDirectory).toAbsolutePath();
        log.info("Searching for {}s in {}", pluginType, path);

        ImmutableMap.Builder<String, PluginMetadata<Definition>> definitions = ImmutableMap.builder();
        if (!path.toFile().exists()) {
            log.warn("{} directory not found", StringUtils.capitalize(pluginType));
            return definitions.build();
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, "*.nar")) {
            for (Path archive : stream) {
                try {
                    Definition def = getPluginDefinition(archive.toString(), narExtractionDirectory);
                    log.info("Found " + pluginType + " from {} : {}", archive, def);

                    checkArgument(StringUtils.isNotBlank(def.getName()));
                    checkArgument(StringUtils.isNotBlank(def.getPluginClassName()));

                    PluginMetadata<Definition> metadata = new PluginMetadata<>();
                    metadata.setDefinition(def);
                    metadata.setArchivePath(archive);

                    definitions.put(def.getName(), metadata);
                } catch (Throwable t) {
                    log.warn("Failed to load {} from {}."
                            + " It is OK however if you want to use this {},"
                            + " please make sure you put the correct {} NAR"
                            + " package in the {} directory ({}).",
                            pluginType, archive, pluginType, pluginType, pluginType, pluginsDirectory, t);
                }
            }
        }

        return definitions.build();
    }

    /**
     * Load the plugin instance according to the definition.
     *
     * @param metadata the plugin definition.
     * @return
     */
    protected PluginWithClassLoader load(PluginMetadata<Definition> metadata,
                               String narExtractionDirectory) throws IOException {
        final File narFile = metadata.getArchivePath().toAbsolutePath().toFile();
        NarClassLoader ncl = NarClassLoaderBuilder.builder()
                .narFile(narFile)
                .parentClassLoader(pluginClass.getClassLoader())
                .extractionDirectory(narExtractionDirectory)
                .build();

        PluginDefinition pluginDefinition = getPluginDefinition(ncl);
        if (StringUtils.isBlank(pluginDefinition.getPluginClassName())) {
            throw new IOException("The " + pluginType + " `" + pluginDefinition.getName() + "` does NOT provide a "
                    + pluginType + " implementation");
        }

        try {
            Class<?> implementation = ncl.loadClass(pluginDefinition.getPluginClassName());
            Object pluginInstance = implementation.getDeclaredConstructor().newInstance();
            if (!this.pluginClass.isInstance(pluginInstance)) {
                throw new IOException("Class " + pluginDefinition.getPluginClassName()
                        + " does not implement " + this.pluginClass.getSimpleName() + " interface");
            }
            Plugin plugin = (Plugin) pluginInstance;
            return wrapper.apply(plugin, ncl);
        } catch (Exception e) {
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            log.error("Failed to load class {}", pluginDefinition.getPluginClassName(), e);
            throw new IOException(e);
        }
    }


    public interface NarClassLoaderFactory {
        NarClassLoader createLoader(String narPath, String narExtractionDirectory) throws IOException;
    }

}
