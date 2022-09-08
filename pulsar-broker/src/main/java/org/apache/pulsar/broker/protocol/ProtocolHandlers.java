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
package org.apache.pulsar.broker.protocol;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.common.plugin.PluginLoader;

/**
 * A collection of loaded handlers.
 */
@Slf4j
public class ProtocolHandlers implements AutoCloseable {

    static final String PULSAR_PROTOCOL_HANDLER_DEFINITION_FILE = "pulsar-protocol-handler.yml";

    @Getter
    private final Map<SocketAddress, String> endpoints = new ConcurrentHashMap<>();

    /**
     * Load the protocol handlers for the given <tt>protocol</tt> list.
     *
     * @param conf the pulsar broker service configuration
     * @return the collection of protocol handlers
     */
    public static ProtocolHandlers load(ServiceConfiguration conf) throws IOException {
        return new ProtocolHandlers(
                new PluginLoader<>(
                        "protocol handler",
                        ProtocolHandler.class,
                        ProtocolHandlerDefinition.class,
                        ProtocolHandlerWithClassLoader::new,
                        conf.getProtocolHandlerDirectory(),
                        conf.getNarExtractionDirectory(),
                        conf.getMessagingProtocols(),
                        PULSAR_PROTOCOL_HANDLER_DEFINITION_FILE) {
                    @Override
                    protected void doubleCheck(String protocol, ProtocolHandlerWithClassLoader instance) {
                        if (!instance.accept(protocol)) {
                            instance.close();
                            log.error("Malformed protocol handler found for protocol `" + protocol + "`");
                            throw new RuntimeException(
                                    "Malformed protocol handler found for protocol `" + protocol + "`");
                        }
                    }
                }.load());
    }

    private final Map<String, ProtocolHandlerWithClassLoader> handlers;

    ProtocolHandlers(Map<String, ProtocolHandlerWithClassLoader> handlers) {
        this.handlers = handlers;
    }

    /**
     * Return the handler for the provided <tt>protocol</tt>.
     *
     * @param protocol the protocol to use
     * @return the protocol handler to handle the provided protocol
     */
    public ProtocolHandler protocol(String protocol) {
        ProtocolHandlerWithClassLoader h = handlers.get(protocol);
        if (null == h) {
            return null;
        } else {
            return h.getHandler();
        }
    }

    public void initialize(ServiceConfiguration conf) throws Exception {
        for (ProtocolHandler handler : handlers.values()) {
            handler.initialize(conf);
        }
    }

    public Map<String, String> getProtocolDataToAdvertise() {
        return handlers.entrySet().stream()
            .collect(Collectors.toMap(
                e -> e.getKey(),
                e -> e.getValue().getProtocolDataToAdvertise()
            ));
    }

    public Map<String, Map<InetSocketAddress, ChannelInitializer<SocketChannel>>> newChannelInitializers() {
        Map<String, Map<InetSocketAddress, ChannelInitializer<SocketChannel>>> channelInitializers = new HashMap<>();
        Set<InetSocketAddress> addresses = new HashSet<>();

        for (Map.Entry<String, ProtocolHandlerWithClassLoader> handler : handlers.entrySet()) {
            Map<InetSocketAddress, ChannelInitializer<SocketChannel>> initializers =
                handler.getValue().newChannelInitializers();
            initializers.forEach((address, initializer) -> {
                if (!addresses.add(address)) {
                    log.error("Protocol handler for `{}` attempts to use {} for its listening port."
                        + " But it is already occupied by other message protocols.",
                        handler.getKey(), address);
                    throw new RuntimeException("Protocol handler for `" + handler.getKey()
                        + "` attempts to use " + address + " for its listening port. But it is"
                        + " already occupied by other messaging protocols");
                }
                channelInitializers.put(handler.getKey(), initializers);
                endpoints.put(address, handler.getKey());
            });
        }

        return channelInitializers;
    }

    public void start(BrokerService service) {
        handlers.values().forEach(handler -> handler.start(service));
    }

    @Override
    public void close() {
        handlers.values().forEach(ProtocolHandler::close);
    }
}
