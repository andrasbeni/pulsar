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
package org.apache.pulsar.metadata.coordination.impl;

import java.util.EnumSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataSerde;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import org.apache.pulsar.metadata.api.MetadataStoreException.LockBusyException;
import org.apache.pulsar.metadata.api.coordination.ResourceLock;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class ResourceLockImpl<T> implements ResourceLock<T> {

    private final MetadataStoreExtended store;
    private final MetadataSerde<T> serde;
    private final String path;

    private volatile T value;
    private long version;
    private final CompletableFuture<Void> expiredFuture;
    private boolean revalidateAfterReconnection = false;
    private CompletableFuture<Void> revalidateFuture;

    private enum State {
        Init,
        Valid,
        Releasing,
        Released,
    }

    private State state;

    public ResourceLockImpl(MetadataStoreExtended store, MetadataSerde<T> serde, String path) {
        this.store = store;
        this.serde = serde;
        this.path = path;
        this.version = -1;
        log.info("\uD83E\uDEB2 - ctor: version: {}", version);
        this.expiredFuture = new CompletableFuture<>();
        this.state = State.Init;
    }

    @Override
    public synchronized T getValue() {
        return value;
    }

    @Override
    public synchronized CompletableFuture<Void> updateValue(T newValue) {
       return acquire(newValue);
    }

    @Override
    public synchronized CompletableFuture<Void> release() {
        log.info("\uD83E\uDEB2 - release ({})", state);

        if (state == State.Released) {
            return CompletableFuture.completedFuture(null);
        }

        state = State.Releasing;
        CompletableFuture<Void> result = new CompletableFuture<>();
        log.info("\uD83E\uDEB2 - calling delete ({})", version);

        store.delete(path, Optional.of(version))
                .thenRun(() -> {
                    synchronized (ResourceLockImpl.this) {
                        state = State.Released;
                    }
                    expiredFuture.complete(null);
                    result.complete(null);
                }).exceptionally(ex -> {
                    log.info("\uD83E\uDEB2 - delete on release failed with exception: {}", ex);

                    if (ex.getCause() instanceof MetadataStoreException.NotFoundException) {
                        // The lock is not there on release. We can anyway proceed
                        synchronized (ResourceLockImpl.this) {
                            state = State.Released;
                        }
                        expiredFuture.complete(null);
                        result.complete(null);
                    } else {
                        result.completeExceptionally(ex);
                    }
                    return null;
                });

        return result;
    }

    @Override
    public CompletableFuture<Void> getLockExpiredFuture() {
        return expiredFuture;
    }

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public int hashCode() {
        return path.hashCode();
    }

    synchronized CompletableFuture<Void> acquire(T newValue) {
        log.info("\uD83E\uDEB2 - Acquire {}", newValue);

        CompletableFuture<Void> result = new CompletableFuture<>();
        acquireWithNoRevalidation(newValue)
                .thenRun(() -> result.complete(null))
                .exceptionally(ex -> {
                    if (ex.getCause() instanceof LockBusyException) {
                        revalidate(newValue, false)
                                .thenAccept(__ -> result.complete(null))
                                .exceptionally(ex1 -> {
                                   result.completeExceptionally(ex1);
                                   return null;
                                });
                    } else {
                        result.completeExceptionally(ex.getCause());
                    }
                    return null;
                });

        return result;
    }

    // Simple operation of acquiring the lock with no retries, or checking for the lock content
    CompletableFuture<Void> acquireWithNoRevalidation(T newValue) {
       // if (log.isDebugEnabled()) {
            log.info("\uD83E\uDEB2 - acquireWithNoRevalidation,newValue={},version={}", newValue, version);
       // }
        byte[] payload;
        try {
            payload = serde.serialize(path, newValue);
        } catch (Throwable t) {
            return FutureUtils.exception(t);
        }

        CompletableFuture<Void> result = new CompletableFuture<>();
        store.put(path, payload, Optional.of(version), EnumSet.of(CreateOption.Ephemeral))
                .thenAccept(stat -> {
                    synchronized (ResourceLockImpl.this) {
                        state = State.Valid;
                        version = stat.getVersion();
                        log.info("\uD83E\uDEB2 - put success: version: {}", version);
                        value = newValue;
                    }
                    log.info("\uD83E\uDEB2 - Acquired resource lock on {}", path);
                    result.complete(null);
                }).exceptionally(ex -> {
            if (ex.getCause() instanceof BadVersionException) {
                result.completeExceptionally(
                        new LockBusyException("Resource at " + path + " is already locked"));
            } else {
                result.completeExceptionally(ex.getCause());
            }
            return null;
        });

        return result;
    }

    synchronized void lockWasInvalidated() {
        if (state != State.Valid) {
            // Ignore notifications while we're releasing the lock ourselves
            return;
        }

        log.info("\uD83E\uDEB2 - Lock on resource {} was invalidated", path);
        revalidate(value, true)
                .thenRun(() -> log.info("\uD83E\uDEB2 - Successfully revalidated the lock on {}", path));
    }

    synchronized CompletableFuture<Void> revalidateIfNeededAfterReconnection() {
        if (revalidateAfterReconnection) {
            revalidateAfterReconnection = false;
            log.warn("\uD83E\uDEB2 - Revalidate lock at {} after reconnection", path);
            return revalidate(value, true);
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    synchronized CompletableFuture<Void> revalidate(T newValue, boolean revalidateAfterReconnection) {
        log.warn("\uD83E\uDEB2 - Revalidate lock at {}", path);

        if (revalidateFuture == null || revalidateFuture.isDone()) {
            revalidateFuture = doRevalidate(newValue);
        } else {
         //   if (log.isDebugEnabled()) {
                log.info("\uD83E\uDEB2 - Previous revalidating is not finished while revalidate newValue={}, "
                                + "value={}, version={}",
                        newValue, value, version);
            //}
            CompletableFuture<Void> newFuture = new CompletableFuture<>();
            revalidateFuture.whenComplete((unused, throwable) -> {
                log.warn("\uD83E\uDEB2 - RevalidateF complete {}", path);

                doRevalidate(newValue).thenRun(() -> newFuture.complete(null))
                        .exceptionally(throwable1 -> {
                            newFuture.completeExceptionally(throwable1);
                            return null;
                        });
            });
            revalidateFuture = newFuture;
        }
        revalidateFuture.exceptionally(ex -> {
            synchronized (ResourceLockImpl.this) {
                Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                if (!revalidateAfterReconnection || realCause instanceof BadVersionException
                        || realCause instanceof LockBusyException) {
                    log.warn("\uD83E\uDEB2 - Failed to revalidate the lock at {}. Marked as expired. {}",
                            path, realCause.getMessage());
                    state = State.Released;
                    expiredFuture.complete(null);
                } else {
                    // We failed to revalidate the lock due to connectivity issue
                    // Continue assuming we hold the lock, until we can revalidate it, either
                    // on Reconnected or SessionReestablished events.
                    ResourceLockImpl.this.revalidateAfterReconnection = true;
                    log.warn("\uD83E\uDEB2 - Failed to revalidate the lock at {}. Retrying later on reconnection {}",
                            path, realCause.getMessage());
                }
            }
            return null;
        });
        return revalidateFuture;
    }

    synchronized CompletableFuture<Void> doRevalidate(T newValue) {
      //  if (log.isDebugEnabled()) {
            log.info("doRevalidate with newValue={}, version={}", newValue, version);
     //   }
        return store.get(path)
                .thenCompose(optGetResult -> {
                    if (!optGetResult.isPresent()) {
                        // The lock just disappeared, try to acquire it again
                        // Reset the expectation on the version
                        setVersion(-1L);
                        log.info("\uD83E\uDEB2 - doRevalidate: version: {}", -1);
                        return acquireWithNoRevalidation(newValue)
                                .thenRun(() -> log.info("Successfully re-acquired missing lock at {}", path));
                    }

                    GetResult res = optGetResult.get();
                    if (!res.getStat().isEphemeral()) {
                        return FutureUtils.exception(
                                new LockBusyException(
                                        "Path " + path + " is already created as non-ephemeral"));
                    }

                    T existingValue;
                    try {
                        existingValue = serde.deserialize(path, res.getValue(), res.getStat());
                    } catch (Throwable t) {
                        return FutureUtils.exception(t);
                    }

                    synchronized (ResourceLockImpl.this) {
                        if (newValue.equals(existingValue)) {
                            // The lock value is still the same, that means that we're the
                            // logical "owners" of the lock.

                            if (res.getStat().isCreatedBySelf()) {
                                // If the new lock belongs to the same session, there's no
                                // need to recreate it.
                                version = res.getStat().getVersion();
                                log.info("\uD83E\uDEB2 - get success: version: {}", version);
                                value = newValue;
                                return CompletableFuture.completedFuture(null);
                            } else {
                                // The lock needs to get recreated since it belong to an earlier
                                // session which maybe expiring soon
                                log.info("Deleting stale lock at {}", path);
                                return store.delete(path, Optional.of(res.getStat().getVersion()))
                                        .thenRun(() -> {
                                            // Reset the expectation that the key is not there anymore
                                            setVersion(-1L);
                                            log.info("\uD83E\uDEB2 - stale: version: {}", -1);
                                        }
                                        )
                                        .thenCompose(__ -> acquireWithNoRevalidation(newValue))
                                        .thenRun(() -> log.info("Successfully re-acquired stale lock at {}", path));
                            }
                        }

                        // At this point we have an existing lock with a value different to what we
                        // expect. If our session is the owner, we can recreate, otherwise the
                        // lock has been acquired by someone else and we give up.

                        if (!res.getStat().isCreatedBySelf()) {
                            return FutureUtils.exception(
                                    new LockBusyException("Resource at " + path + " is already locked"));
                        }

                        return store.delete(path, Optional.of(res.getStat().getVersion()))
                                .thenRun(() -> {
                                    // Reset the expectation that the key is not there anymore
                                    setVersion(-1L);
                                            log.info("\uD83E\uDEB2 - before recreate: version: {}", -1);
                                }
                                )
                                .thenCompose(__ -> acquireWithNoRevalidation(newValue))
                                .thenRun(() -> log.info("Successfully re-acquired lock at {}", path));
                    }
                });
    }

    private synchronized void setVersion(long version) {
        this.version = version;
    }



    static class LoggingLock<T> extends ResourceLockImpl<T> {
        private final Logger log = LoggerFactory.getLogger("wrapper\uD83C\uDF81");

        public LoggingLock(MetadataStoreExtended store, MetadataSerde<T> serde, String path) {
            super(store, serde, path);
            log.info("Constructor");
        }


        @Override
        public String getPath() {
            log.info("getPath");
            return super.getPath();
        }

        @Override
        public T getValue() {
            log.info("getValue");
            return super.getValue();
        }

        @Override
        public CompletableFuture<Void> updateValue(T newValue) {
            log.info("updateValue({})", newValue);
            return super.updateValue(newValue);
        }

        @Override
        public CompletableFuture<Void> release() {
            log.info("release");
            return super.release();
        }

        @Override
        public CompletableFuture<Void> getLockExpiredFuture() {
            log.info("getFuture");
            return super.getLockExpiredFuture();
        }






















        @Override
        public int hashCode() {
            log.info("hashCode()");
            return super.hashCode();
        }

        synchronized CompletableFuture<Void> acquire(T newValue) {
            log.info("acquire({})", newValue);
            return super.acquire(newValue);
        }

        // Simple operation of acquiring the lock with no retries, or checking for the lock content
        CompletableFuture<Void> acquireWithNoRevalidation(T newValue) {
            log.info("acquireWithNoRevalidation({})", newValue);
            return super.acquireWithNoRevalidation(newValue);
        }

        synchronized void lockWasInvalidated() {
            log.info("lockWasInvalidated()");
            super.lockWasInvalidated();
        }

        synchronized CompletableFuture<Void> revalidateIfNeededAfterReconnection() {
            log.info("revalidateIfNeededAfterReconnection()");
            return super.revalidateIfNeededAfterReconnection();
        }

        synchronized CompletableFuture<Void> revalidate(T newValue, boolean revalidateAfterReconnection) {
            log.info("revalidate({}, {})", newValue, revalidateAfterReconnection);
            return super.revalidate(newValue, revalidateAfterReconnection);
        }

        synchronized CompletableFuture<Void> doRevalidate(T newValue) {
            log.info("doRevalidate({})", newValue);
            return super.doRevalidate(newValue);
        }

    }






}
