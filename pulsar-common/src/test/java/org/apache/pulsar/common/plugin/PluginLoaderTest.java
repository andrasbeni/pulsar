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

import org.apache.pulsar.common.nar.NarClassLoader;
import org.mockito.Answers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.io.File;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Test
public class PluginLoaderTest {

    private static final String NAR_PATH = "/mock/narExtraction/directory";
    private static final String PLUGIN_DIRECTORY = "/mock/plugin/directory";

    private MockedStatic<Files> mockedFiles;
    private MockedStatic<Paths> mockedPaths;

    private List<String> nars;
    private Path pluginDirectoryPath;
    private NarClassLoader classLoader;
    private NarClassLoader classLoaderForNar1;
    private NarClassLoader classLoaderForNar2;
    private PluginLoader<MockPluginInterface, MockPluginDefinition, MockPluginWithClassLoader> loader;
    private Map<String, Path> narPaths;


    @BeforeMethod
    public void setup() throws Exception {
        classLoader = mock(NarClassLoader.class);
        classLoaderForNar1 = mock(NarClassLoader.class);
        classLoaderForNar2 = mock(NarClassLoader.class);
        PluginLoader.NarClassLoaderFactory mockClassLoaderFactory = (narPath, narExtractionDirectory) -> {
            if ((PLUGIN_DIRECTORY+"/nar1.nar").equals(narPath)) {
                return classLoaderForNar1;
            } else if ((PLUGIN_DIRECTORY+"/nar2.nar").equals(narPath)) {
                return classLoaderForNar2;
            }
            return classLoader;
        };

        mockedFiles = Mockito.mockStatic(Files.class);
        pluginDirectoryPath = mock(Path.class);
        when(pluginDirectoryPath.toAbsolutePath()).thenReturn(pluginDirectoryPath);
        File pluginDirectoryFile = mock(File.class);
        when(pluginDirectoryPath.toFile()).thenReturn(pluginDirectoryFile);
        when(pluginDirectoryFile.exists()).thenReturn(true);
        Path narExtractionDirectoryPath = mock(Path.class, Answers.RETURNS_MOCKS);
        when(narExtractionDirectoryPath.toAbsolutePath()).thenReturn(narExtractionDirectoryPath);
        when(narExtractionDirectoryPath.toFile().exists()).thenReturn(true);
        nars = new ArrayList<>(Arrays.asList("nar1.nar", "nar2.nar"));
        narPaths = nars.stream().collect(Collectors.toMap(name->name, name -> {
                    Path mockNarPath = mock(Path.class);
                    when(mockNarPath.toString()).thenReturn(PLUGIN_DIRECTORY + "/" + name);
                    return mockNarPath;
                }));

        mockedPaths = Mockito.mockStatic(Paths.class);
        mockedPaths.when(() -> Paths.get(PLUGIN_DIRECTORY)).thenReturn(pluginDirectoryPath);
        mockedFiles.when(() -> Files.newDirectoryStream(pluginDirectoryPath, "*.nar")).thenReturn(
                new DirectoryStream<Path>() {
                    @Override
                    public Iterator<Path> iterator() {
                        return narPaths.values().iterator();
                    }
                    @Override
                    public void close() { }
                });
        loader =
                new PluginLoader<>(
                        "mock",
                        MockPluginInterface.class,
                        MockPluginDefinition.class,
                        MockPluginWithClassLoader::new,
                        PLUGIN_DIRECTORY,
                        NAR_PATH,
                        Arrays.asList("myMock1", "myMock2"),
                        "mockFile.yaml", "mockFile.yml");
        loader.setClassLoaderFactory(mockClassLoaderFactory);
    }

    @AfterMethod
    public void cleanup() {
        mockedFiles.close();
        mockedPaths.close();
    }


    @Test
    public void testGetPluginDefinitionNoFile()  throws Exception {
        when(classLoader.getServiceDefinition(any())).thenThrow(new NoSuchFileException(""));
        try {
            loader.getPluginDefinition(classLoader);
            Assert.fail("Should have failed");
        } catch (NoSuchFileException e) {
            // Expected exception
        }
    }

    @Test
    public void testGetPluginDefinitionFirstFileOnly()  throws Exception {
        MockPluginDefinition expectedDefinition = serviceDefinition("myMock1");
        when(classLoader.getServiceDefinition("mockFile.yaml")).thenReturn(expectedDefinition.toYaml());
        when(classLoader.getServiceDefinition("mockFile.yml")).thenThrow(new NoSuchFileException(""));
        MockPluginDefinition definition = loader.getPluginDefinition(classLoader);
        Assert.assertEquals(definition, expectedDefinition);
    }

    @Test
    public void testGetPluginDefinitionSecondFileOnly()  throws Exception {
        MockPluginDefinition expectedDefinition = serviceDefinition("myMock1");
        when(classLoader.getServiceDefinition("mockFile.yaml")).thenThrow(new NoSuchFileException(""));
        when(classLoader.getServiceDefinition("mockFile.yml")).thenReturn(expectedDefinition.toYaml());
        MockPluginDefinition definition = loader.getPluginDefinition(classLoader);
        Assert.assertEquals(definition, expectedDefinition);
    }

    @Test
    public void testGetPluginDefinitionBothExist()  throws Exception {
        MockPluginDefinition expectedDefinition = serviceDefinition("myMock1");
        when(classLoader.getServiceDefinition("mockFile.yaml")).thenReturn(expectedDefinition.toYaml());
        when(classLoader.getServiceDefinition("mockFile.yml")).thenReturn(serviceDefinition("myMock2").toYaml());
        MockPluginDefinition definition = loader.getPluginDefinition(classLoader);
        Assert.assertEquals(definition, expectedDefinition);
    }

    @Test
    public void testGetPluginDefinitionThrowsWhenInvalidYaml() throws Exception {
        when(classLoader.getServiceDefinition("mockFile.yaml")).thenReturn("\"");
        try {
            loader.getPluginDefinition(classLoader);
            Assert.fail("Should have thrown exception");
        } catch (Exception e) {
            // Expected exception
        }
    }

    @Test
    public void testGetPluginDefinition() throws Exception {
        MockPluginDefinition expectedDefinition = serviceDefinition("myMock1");
        when(classLoader.getServiceDefinition("mockFile.yaml")).thenReturn(expectedDefinition.toYaml());
        String fileName = PLUGIN_DIRECTORY + "/1.nar";
        MockPluginDefinition result = loader.getPluginDefinition(fileName, NAR_PATH);
        Assert.assertEquals(result, expectedDefinition);
    }

    @Test
    public void testSearchForPluginDefinitionsExtractionDirDoesNotExist() throws Exception {
        Path pluginDirectoryPath2 = mock(Path.class, Answers.RETURNS_MOCKS);
        when(pluginDirectoryPath2.toFile().exists()).thenReturn(false);
        mockedPaths.when(() -> Paths.get("/no/such/directory")).thenReturn(pluginDirectoryPath2);
        Map<String, PluginMetadata<MockPluginDefinition>> map =
                loader.searchForPluginDefinitions("/no/such/directory");
        Assert.assertEquals(Collections.emptyMap(), map);
    }

    @Test
    public void testSearchForPluginDefinitionsExtractionDirIsEmpty() throws Exception {
        nars.clear();
        Map<String, PluginMetadata<MockPluginDefinition>> map =
                loader.searchForPluginDefinitions(NAR_PATH);
        Assert.assertEquals(Collections.emptyMap(), map);
    }

    @Test
    public void testSearchForPluginDefinitions() throws Exception {
        when(classLoaderForNar1.getServiceDefinition(any())).thenReturn(serviceDefinition("myMock1").toYaml());
        when(classLoaderForNar2.getServiceDefinition(any())).thenThrow(new NoSuchFileException(""));

        Map<String, PluginMetadata<MockPluginDefinition>> map =
                loader.searchForPluginDefinitions(PLUGIN_DIRECTORY);
        Assert.assertEquals(Collections.singletonMap("myMock1",
                metadata(serviceDefinition("myMock1"), narPaths.get("nar1.nar"))), map);
    }



    private PluginMetadata<MockPluginDefinition> metadata(
            MockPluginDefinition definition, Path path) {
        PluginMetadata<MockPluginDefinition> metadata = new PluginMetadata<>();
        metadata.setArchivePath(path);
        metadata.setDefinition(definition);
        return metadata;
    }


    private MockPluginDefinition serviceDefinition(String name) {
        MockPluginDefinition def = new MockPluginDefinition();
        def.setName(name);
        def.setDescription("desc. of " + name);
        def.setPluginClass(MockPluginImplementation.class.getName());
        return def;

    }


}
