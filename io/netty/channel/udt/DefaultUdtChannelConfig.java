/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.channel.udt;

import com.barchart.udt.OptionUDT;
import com.barchart.udt.SocketUDT;
import com.barchart.udt.nio.ChannelUDT;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.channel.DefaultChannelConfig;
import io.netty.channel.MessageSizeEstimator;
import io.netty.channel.RecvByteBufAllocator;

import java.io.IOException;
import java.util.Map;

import static io.netty.channel.udt.UdtChannelOption.*;

/**
 * The default {@link UdtChannelConfig} implementation.
 */
public class DefaultUdtChannelConfig extends DefaultChannelConfig implements
        UdtChannelConfig {

    private static final int K = 1024;
    private static final int M = K * K;

    private volatile int protocolReceiveBuferSize = 10 * M;
    private volatile int protocolSendBuferSize = 10 * M;

    private volatile int systemReceiveBufferSize = M;
    private volatile int systemSendBuferSize = M;

    private volatile int allocatorReceiveBufferSize = 128 * K;
    private volatile int allocatorSendBufferSize = 128 * K;

    private volatile int soLinger;

    private volatile boolean reuseAddress = true;

    public DefaultUdtChannelConfig(final UdtChannel channel,
            final ChannelUDT channelUDT, final boolean apply)
            throws IOException {
        super(channel);
        if (apply) {
            apply(channelUDT);
        }
    }

    protected void apply(final ChannelUDT channelUDT) throws IOException {
        final SocketUDT socketUDT = channelUDT.socketUDT();
        socketUDT.setReuseAddress(isReuseAddress());
        socketUDT.setSendBufferSize(getSendBufferSize());
        if (getSoLinger() <= 0) {
            socketUDT.setSoLinger(false, 0);
        } else {
            socketUDT.setSoLinger(true, getSoLinger());
        }
        socketUDT.setOption(OptionUDT.Protocol_Receive_Buffer_Size,
                getProtocolReceiveBufferSize());
        socketUDT.setOption(OptionUDT.Protocol_Send_Buffer_Size,
                getProtocolSendBufferSize());
        socketUDT.setOption(OptionUDT.System_Receive_Buffer_Size,
                getSystemReceiveBufferSize());
        socketUDT.setOption(OptionUDT.System_Send_Buffer_Size,
                getSystemSendBufferSize());
    }

    @Override
    public int getProtocolReceiveBufferSize() {
        return protocolReceiveBuferSize;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getOption(final ChannelOption<T> option) {
        if (option == PROTOCOL_RECEIVE_BUFFER_SIZE) {
            return (T) Integer.valueOf(getProtocolReceiveBufferSize());
        }
        if (option == PROTOCOL_SEND_BUFFER_SIZE) {
            return (T) Integer.valueOf(getProtocolSendBufferSize());
        }
        if (option == SYSTEM_RECEIVE_BUFFER_SIZE) {
            return (T) Integer.valueOf(getSystemReceiveBufferSize());
        }
        if (option == SYSTEM_SEND_BUFFER_SIZE) {
            return (T) Integer.valueOf(getSystemSendBufferSize());
        }
        if (option == SO_RCVBUF) {
            return (T) Integer.valueOf(getReceiveBufferSize());
        }
        if (option == SO_SNDBUF) {
            return (T) Integer.valueOf(getSendBufferSize());
        }
        if (option == SO_REUSEADDR) {
            return (T) Boolean.valueOf(isReuseAddress());
        }
        if (option == SO_LINGER) {
            return (T) Integer.valueOf(getSoLinger());
        }
        return super.getOption(option);
    }

    @Override
    public Map<ChannelOption<?>, Object> getOptions() {
        return getOptions(super.getOptions(), PROTOCOL_RECEIVE_BUFFER_SIZE,
                PROTOCOL_SEND_BUFFER_SIZE, SYSTEM_RECEIVE_BUFFER_SIZE,
                SYSTEM_SEND_BUFFER_SIZE, SO_RCVBUF, SO_SNDBUF, SO_REUSEADDR,
                SO_LINGER);
    }

    @Override
    public int getReceiveBufferSize() {
        return allocatorReceiveBufferSize;
    }

    @Override
    public int getSendBufferSize() {
        return allocatorSendBufferSize;
    }

    @Override
    public int getSoLinger() {
        return soLinger;
    }

    @Override
    public boolean isReuseAddress() {
        return reuseAddress;
    }

    @Override
    public UdtChannelConfig setProtocolReceiveBufferSize(final int protocolReceiveBuferSize) {
        this.protocolReceiveBuferSize = protocolReceiveBuferSize;
        return this;
    }

    @Override
    public <T> boolean setOption(final ChannelOption<T> option, final T value) {
        validate(option, value);
        if (option == PROTOCOL_RECEIVE_BUFFER_SIZE) {
            setProtocolReceiveBufferSize((Integer) value);
        } else if (option == PROTOCOL_SEND_BUFFER_SIZE) {
            setProtocolSendBufferSize((Integer) value);
        } else if (option == SYSTEM_RECEIVE_BUFFER_SIZE) {
            setSystemReceiveBufferSize((Integer) value);
        } else if (option == SYSTEM_SEND_BUFFER_SIZE) {
            setSystemSendBufferSize((Integer) value);
        } else if (option == SO_RCVBUF) {
            setReceiveBufferSize((Integer) value);
        } else if (option == SO_SNDBUF) {
            setSendBufferSize((Integer) value);
        } else if (option == SO_REUSEADDR) {
            setReuseAddress((Boolean) value);
        } else if (option == SO_LINGER) {
            setSoLinger((Integer) value);
        } else {
            return super.setOption(option, value);
        }
        return true;
    }

    @Override
    public UdtChannelConfig setReceiveBufferSize(final int receiveBufferSize) {
        allocatorReceiveBufferSize = receiveBufferSize;
        return this;
    }

    @Override
    public UdtChannelConfig setReuseAddress(final boolean reuseAddress) {
        this.reuseAddress = reuseAddress;
        return this;
    }

    @Override
    public UdtChannelConfig setSendBufferSize(final int sendBufferSize) {
        allocatorSendBufferSize = sendBufferSize;
        return this;
    }

    @Override
    public UdtChannelConfig setSoLinger(final int soLinger) {
        this.soLinger = soLinger;
        return this;
    }

    @Override
    public int getSystemReceiveBufferSize() {
        return systemReceiveBufferSize;
    }

    @Override
    public UdtChannelConfig setSystemSendBufferSize(
            final int systemReceiveBufferSize) {
        this.systemReceiveBufferSize = systemReceiveBufferSize;
        return this;
    }

    @Override
    public int getProtocolSendBufferSize() {
        return protocolSendBuferSize;
    }

    @Override
    public UdtChannelConfig setProtocolSendBufferSize(
            final int protocolSendBuferSize) {
        this.protocolSendBuferSize = protocolSendBuferSize;
        return this;
    }

    @Override
    public UdtChannelConfig setSystemReceiveBufferSize(
            final int systemSendBuferSize) {
        this.systemSendBuferSize = systemSendBuferSize;
        return this;
    }

    @Override
    public int getSystemSendBufferSize() {
        return systemSendBuferSize;
    }

    @Override
    public UdtChannelConfig setConnectTimeoutMillis(int connectTimeoutMillis) {
        super.setConnectTimeoutMillis(connectTimeoutMillis);
        return this;
    }

    @Override
    public UdtChannelConfig setMaxMessagesPerRead(int maxMessagesPerRead) {
        super.setMaxMessagesPerRead(maxMessagesPerRead);
        return this;
    }

    @Override
    public UdtChannelConfig setWriteSpinCount(int writeSpinCount) {
        super.setWriteSpinCount(writeSpinCount);
        return this;
    }

    @Override
    public UdtChannelConfig setAllocator(ByteBufAllocator allocator) {
        super.setAllocator(allocator);
        return this;
    }

    @Override
    public UdtChannelConfig setRecvByteBufAllocator(RecvByteBufAllocator allocator) {
        super.setRecvByteBufAllocator(allocator);
        return this;
    }

    @Override
    public UdtChannelConfig setAutoRead(boolean autoRead) {
        super.setAutoRead(autoRead);
        return this;
    }

    @Override
    public UdtChannelConfig setAutoClose(boolean autoClose) {
        super.setAutoClose(autoClose);
        return this;
    }

    @Override
    public UdtChannelConfig setWriteBufferLowWaterMark(int writeBufferLowWaterMark) {
        super.setWriteBufferLowWaterMark(writeBufferLowWaterMark);
        return this;
    }

    @Override
    public UdtChannelConfig setWriteBufferHighWaterMark(int writeBufferHighWaterMark) {
        super.setWriteBufferHighWaterMark(writeBufferHighWaterMark);
        return this;
    }

    @Override
    public UdtChannelConfig setMessageSizeEstimator(MessageSizeEstimator estimator) {
        super.setMessageSizeEstimator(estimator);
        return this;
    }
}
