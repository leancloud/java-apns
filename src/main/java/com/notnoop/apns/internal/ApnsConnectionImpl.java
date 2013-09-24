/*
 * Copyright 2009, Mahmood Ali. All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:
 * 
 * * Redistributions of source code must retain the above copyright notice, this list of conditions
 * and the following disclaimer. * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the documentation and/or other
 * materials provided with the distribution. * Neither the name of Mahmood Ali. nor the names of its
 * contributors may be used to endorse or promote products derived from this software without
 * specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.notnoop.apns.internal;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.notnoop.apns.ApnsDelegate;
import com.notnoop.apns.ApnsNotification;
import com.notnoop.apns.DeliveryError;
import com.notnoop.apns.ReconnectPolicy;
import com.notnoop.apns.SimpleApnsNotification;
import com.notnoop.exceptions.ApnsDeliveryErrorException;
import com.notnoop.exceptions.NetworkIOException;


public class ApnsConnectionImpl implements ApnsConnection {

    private static final Logger logger = LoggerFactory.getLogger(ApnsConnectionImpl.class);
    private final SocketFactory factory;
    private final String host;
    private final int port;
    private final Proxy proxy;
    private final ReconnectPolicy reconnectPolicy;
    private final ApnsDelegate delegate;
    private int cacheLength;
    private final boolean errorDetection;
    private final int maxNotificationBufferSize;
    private final boolean autoAdjustCacheLength;
    private final ConcurrentLinkedQueue<ApnsNotification> cachedNotifications, notificationsBuffer;


    public ApnsConnectionImpl(SocketFactory factory, String host, int port,
                              int maxNotificationBufferSize) {
        this(factory, host, port, new ReconnectPolicies.Never(), ApnsDelegate.EMPTY,
            maxNotificationBufferSize);
    }


    public ApnsConnectionImpl(SocketFactory factory, String host, int port,
                              ReconnectPolicy reconnectPolicy, ApnsDelegate delegate, int maxNotificationBufferSize) {
        this(factory, host, port, null, reconnectPolicy, delegate, false,
            ApnsConnection.DEFAULT_CACHE_LENGTH, true, maxNotificationBufferSize);
    }


    public ApnsConnectionImpl(SocketFactory factory, String host, int port, Proxy proxy,
                              ReconnectPolicy reconnectPolicy, ApnsDelegate delegate, boolean errorDetection,
                              int cacheLength, boolean autoAdjustCacheLength, int maxNotificationBufferSize) {
        this.factory = factory;
        this.host = host;
        this.port = port;
        this.reconnectPolicy = reconnectPolicy;
        this.delegate = delegate == null ? ApnsDelegate.EMPTY : delegate;
        this.proxy = proxy;
        this.errorDetection = errorDetection;
        this.cacheLength = cacheLength;
        this.maxNotificationBufferSize = maxNotificationBufferSize;
        this.autoAdjustCacheLength = autoAdjustCacheLength;
        this.cachedNotifications = new ConcurrentLinkedQueue<ApnsNotification>();
        this.notificationsBuffer = new ConcurrentLinkedQueue<ApnsNotification>();
    }


    public void shrinkResendNotificationBuffer() {
        this.drainBuffer();
        synchronized (this) {
            try {
                while (this.notificationsBuffer.size() > this.maxNotificationBufferSize) {
                    this.wait();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }


    @Override
    public synchronized void close() {
        Utilities.close(this.socket);
    }


    private void monitorSocket(final Socket socket) {
        class MonitoringThread extends Thread {

            @Override
            public void run() {

                try {
                    InputStream in = socket.getInputStream();

                    // TODO(jwilson): this should readFully()
                    final int expectedSize = 6;
                    byte[] bytes = new byte[expectedSize];
                    while (in.read(bytes) == expectedSize) {

                        int command = bytes[0] & 0xFF;
                        if (command != 8) {
                            throw new IOException("Unexpected command byte " + command);
                        }
                        int statusCode = bytes[1] & 0xFF;
                        DeliveryError e = DeliveryError.ofCode(statusCode);

                        int id = Utilities.parseBytes(bytes[2], bytes[3], bytes[4], bytes[5]);

                        Queue<ApnsNotification> tempCache = new LinkedList<ApnsNotification>();
                        ApnsNotification notification = null;
                        boolean foundNotification = false;

                        while (!ApnsConnectionImpl.this.cachedNotifications.isEmpty()) {
                            notification = ApnsConnectionImpl.this.cachedNotifications.poll();

                            if (notification.getIdentifier() == id) {
                                foundNotification = true;
                                break;
                            }
                            tempCache.add(notification);
                        }

                        if (foundNotification) {
                            ApnsConnectionImpl.this.delegate.messageSendFailed(notification,
                                new ApnsDeliveryErrorException(e));
                        } else {
                            ApnsConnectionImpl.this.cachedNotifications.addAll(tempCache);
                            int resendSize = tempCache.size();
                            logger.warn("Received error for message "
                                    + "that wasn't in the cache...");
                            if (ApnsConnectionImpl.this.autoAdjustCacheLength) {
                                ApnsConnectionImpl.this.cacheLength =
                                        ApnsConnectionImpl.this.cacheLength + resendSize / 2;
                                ApnsConnectionImpl.this.delegate
                                .cacheLengthExceeded(ApnsConnectionImpl.this.cacheLength);
                            }
                            ApnsConnectionImpl.this.delegate.messageSendFailed(null,
                                new ApnsDeliveryErrorException(e));
                        }

                        int resendSize = 0;

                        while (!ApnsConnectionImpl.this.cachedNotifications.isEmpty()) {
                            resendSize++;
                            ApnsConnectionImpl.this.notificationsBuffer
                            .add(ApnsConnectionImpl.this.cachedNotifications.poll());
                        }
                        ApnsConnectionImpl.this.delegate.notificationsResent(resendSize);
                        ApnsConnectionImpl.this.delegate.connectionClosed(e, id);
                    }

                } catch (Exception e) {
                    logger.info("Exception while waiting for error code", e);
                    ApnsConnectionImpl.this.delegate.connectionClosed(DeliveryError.UNKNOWN, -1);
                } finally {
                    ApnsConnectionImpl.this.close();
                    // At last,we retry to send error notifications.
                    ApnsConnectionImpl.this.drainBuffer();
                }

            }
        }
        Thread t = new MonitoringThread();
        t.setDaemon(true);
        t.start();
    }

    // This method is only called from sendMessage. sendMessage
    // has the required logic for retrying
    private Socket socket;


    private synchronized Socket socket() throws NetworkIOException {
        if (this.reconnectPolicy.shouldReconnect()) {
            Utilities.close(this.socket);
            this.socket = null;
        }

        if (this.socket == null || this.socket.isClosed()) {
            try {
                if (this.proxy == null) {
                    this.socket = this.factory.createSocket(this.host, this.port);
                } else if (this.proxy.type() == Proxy.Type.HTTP) {
                    TlsTunnelBuilder tunnelBuilder = new TlsTunnelBuilder();
                    this.socket =
                            tunnelBuilder.build((SSLSocketFactory) this.factory, this.proxy,
                                this.host, this.port);
                } else {
                    boolean success = false;
                    Socket proxySocket = null;
                    try {
                        proxySocket = new Socket(this.proxy);
                        proxySocket.connect(new InetSocketAddress(this.host, this.port));
                        this.socket =
                                ((SSLSocketFactory) this.factory).createSocket(proxySocket,
                                    this.host, this.port, false);
                        success = true;
                    } finally {
                        if (!success) {
                            Utilities.close(proxySocket);
                        }
                    }
                }

                if (this.socket != null) {
                    try {
                        this.socket.setSoTimeout(40000);
                        this.socket.setTcpNoDelay(true);
                        this.socket.setReceiveBufferSize(64 * 1024);
                    } catch (IOException e) {
                        // ignore
                    }
                }

                if (this.errorDetection) {
                    this.monitorSocket(this.socket);
                }

                this.reconnectPolicy.reconnected();
                logger.debug("Made a new connection to APNS");
            } catch (IOException e) {
                logger.error("Couldn't connect to APNS server", e);
                throw new NetworkIOException(e);
            }
        }
        return this.socket;
    }

    int DELAY_IN_MS = 1000;
    private static final int RETRIES = 3;


    @Override
    public synchronized void sendMessage(ApnsNotification m) throws NetworkIOException {
        this.shrinkResendNotificationBuffer();
        this.sendMessage(m, false);
    }


    public synchronized void sendMessage(ApnsNotification m, boolean fromBuffer)
            throws NetworkIOException {
        int attempts = 0;
        while (true) {
            try {
                attempts++;
                Socket socket = this.socket();
                OutputStream outputStream = socket.getOutputStream();
                outputStream.write(m.marshall());
                outputStream.flush();
                this.cacheNotification(m);

                this.delegate.messageSent(m, fromBuffer);

                logger.debug("Message \"{}\" sent", m);

                attempts = 0;
                this.drainBuffer();
                break;
            } catch (Exception e) {
                Utilities.close(this.socket);
                this.socket = null;
                if (attempts >= RETRIES) {
                    logger.error("Couldn't send message after " + RETRIES + " retries." + m, e);
                    this.delegate.messageSendFailed(m, e);
                    Utilities.wrapAndThrowAsRuntimeException(e);
                }
                // The first failure might be due to closed connection
                // don't delay quite yet
                if (attempts != 1) {
                    // Do not spam the log files when the APNS server closed the
                    // socket (due to a
                    // bad token, for example), only log when on the second
                    // retry.
                    logger.info("Failed to send message " + m + "... trying again after delay", e);
                    Utilities.sleep(this.DELAY_IN_MS);
                }
            }
        }
    }

    private final Lock resendLock = new ReentrantLock();


    private void drainBuffer() {
        if (this.resendLock.tryLock()) {
            try {
                if (!this.notificationsBuffer.isEmpty()) {
                    this.sendMessage(this.notificationsBuffer.poll(), true);
                }
            } finally {
                this.resendLock.unlock();
            }
            synchronized (this) {
                this.notifyAll();
            }
        }
    }


    private void cacheNotification(ApnsNotification notification) {
        this.cachedNotifications.add(notification);
        while (this.cachedNotifications.size() > this.cacheLength) {
            this.cachedNotifications.poll();
            logger.debug("Removing notification from cache " + notification);
        }
    }


    @Override
    public ApnsConnectionImpl copy() {
        return new ApnsConnectionImpl(this.factory, this.host, this.port, this.proxy,
            this.reconnectPolicy.copy(), this.delegate, this.errorDetection, this.cacheLength,
            this.autoAdjustCacheLength, this.maxNotificationBufferSize);
    }


    @Override
    public void testConnection() throws NetworkIOException {
        ApnsConnectionImpl testConnection = null;
        try {
            testConnection =
                    new ApnsConnectionImpl(this.factory, this.host, this.port,
                        this.reconnectPolicy.copy(), ApnsDelegate.EMPTY,
                        this.maxNotificationBufferSize);
            testConnection.sendMessage(new SimpleApnsNotification(new byte[] {0}, new byte[] {0}));
        } finally {
            if (testConnection != null) {
                testConnection.close();
            }
        }
    }


    @Override
    public void setCacheLength(int cacheLength) {
        this.cacheLength = cacheLength;
    }


    @Override
    public int getCacheLength() {
        return this.cacheLength;
    }
}
