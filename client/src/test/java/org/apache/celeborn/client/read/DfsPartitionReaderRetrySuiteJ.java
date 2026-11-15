/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.client.read;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.client.read.checkpoint.PartitionReaderCheckpointMetadata;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientFactory;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.PbStreamHandler;
import org.apache.celeborn.common.protocol.StorageInfo;
import org.apache.celeborn.common.util.Utils;

public class DfsPartitionReaderRetrySuiteJ {

  private static final int FETCH_PORT = 10001;
  private static final int CHUNK_LENGTH = 1024;
  private static final int FAIL_AT_POSITION = 300;

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() {
    ShuffleClient.reset();
  }

  @After
  public void tearDown() {
    ShuffleClient.reset();
  }

  @Test
  public void testReadChunkResumesFromMissingBytes() throws Exception {
    byte[] chunk = chunkBytes();
    AtomicInteger failuresToInject = new AtomicInteger(2);
    List<Long> readPositions = Collections.synchronizedList(new ArrayList<>());
    DfsPartitionReader reader =
        newReader(chunk, failuresToInject, readPositions, 5, Optional.empty());

    try {
      ByteBuf result = reader.next();
      byte[] actual = new byte[CHUNK_LENGTH];
      result.readBytes(actual);
      result.release();
      assertArrayEquals(chunk, actual);
      // The first stream delivers bytes [0, 300) and then fails. Every retry must
      // resume from offset 300 instead of re-reading the whole chunk from 0.
      assertEquals(Arrays.asList(0L, 300L, 300L, 300L), readPositions);
    } finally {
      reader.close();
    }
  }

  @Test
  public void testReadChunkFailsAfterRetriesExhausted() throws Exception {
    byte[] chunk = chunkBytes();
    AtomicInteger failuresToInject = new AtomicInteger(100);
    List<Long> readPositions = Collections.synchronizedList(new ArrayList<>());
    DfsPartitionReader reader =
        newReader(chunk, failuresToInject, readPositions, 2, Optional.empty());

    ExecutorService caller = Executors.newSingleThreadExecutor();
    Future<ByteBuf> result = caller.submit(reader::next);
    try {
      result.get(10, TimeUnit.SECONDS);
      fail("PartitionReader.next() should fail after read retries are exhausted");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof IOException);
    } catch (TimeoutException e) {
      throw new AssertionError("PartitionReader.next() did not terminate", e);
    } finally {
      result.cancel(true);
      caller.shutdownNow();
      reader.close();
    }
  }

  private DfsPartitionReader newReader(
      byte[] chunk,
      AtomicInteger failuresToInject,
      List<Long> readPositions,
      int maxRetries,
      Optional<PartitionReaderCheckpointMetadata> checkpointMetadata)
      throws Exception {
    File dataFile = temporaryFolder.newFile("partition.data");
    try (FileOutputStream output = new FileOutputStream(dataFile)) {
      output.write(chunk);
    }
    try (DataOutputStream output =
        new DataOutputStream(new FileOutputStream(Utils.getIndexFilePath(dataFile.getPath())))) {
      output.writeInt(2);
      output.writeLong(0L);
      output.writeLong(CHUNK_LENGTH);
    }

    FileSystem realFs = FileSystem.getLocal(new Configuration());
    Path dataPath = new Path(dataFile.getAbsolutePath());
    FileSystem mockFs = mock(FileSystem.class);
    when(mockFs.open(any(Path.class)))
        .thenAnswer(
            invocation -> {
              Path path = invocation.getArgument(0);
              if (path.toString().endsWith(Utils.INDEX_SUFFIX())) {
                return realFs.open(path);
              }
              return new FSDataInputStream(
                  new FlakyPositionedStream(
                      chunk, failuresToInject, FAIL_AT_POSITION, readPositions));
            });
    when(mockFs.getFileStatus(any(Path.class)))
        .thenAnswer(invocation -> realFs.getFileStatus(invocation.getArgument(0)));

    Map<StorageInfo.Type, FileSystem> hadoopFs =
        Collections.singletonMap(StorageInfo.Type.HDFS, mockFs);
    setStaticField(ShuffleClient.class, "hadoopFs", hadoopFs);

    CelebornConf conf = new CelebornConf();
    conf.set("celeborn.client.fetch.dfsReadMaxRetries", String.valueOf(maxRetries));
    conf.set("celeborn.client.fetch.dfsReadRetryWait", "10ms");

    TransportClientFactory clientFactory = mock(TransportClientFactory.class);
    when(clientFactory.createClient(anyString(), anyInt())).thenReturn(mock(TransportClient.class));

    PartitionLocation location =
        new PartitionLocation(
            0, 0, "localhost", 10000, 10002, FETCH_PORT, 10003, PartitionLocation.Mode.PRIMARY);
    location.setStorageInfo(new StorageInfo(StorageInfo.Type.HDFS, true, dataPath.toString()));

    return new DfsPartitionReader(
        conf,
        "app-1",
        location,
        PbStreamHandler.newBuilder().setStreamId(1L).build(),
        clientFactory,
        0,
        Integer.MAX_VALUE,
        new NoOpMetricsCallback(),
        -1,
        -1,
        checkpointMetadata);
  }

  private static byte[] chunkBytes() {
    byte[] chunk = new byte[CHUNK_LENGTH];
    for (int i = 0; i < chunk.length; i++) {
      chunk[i] = (byte) (i % 251);
    }
    return chunk;
  }

  private static void setStaticField(Class<?> targetClass, String name, Object value)
      throws Exception {
    Field field = targetClass.getDeclaredField(name);
    field.setAccessible(true);
    field.set(null, value);
  }

  private static class NoOpMetricsCallback implements MetricsCallback {
    @Override
    public void incBytesRead(long bytesRead) {}

    @Override
    public void incReadTime(long time) {}
  }

  // A positioned-readable stream that delivers bytes only up to a configured
  // position and then fails, simulating a broken connection to remote storage.
  // Each failure consumes one of the shared failuresToInject permits, so streams
  // created after the permits are exhausted read normally.
  private static class FlakyPositionedStream extends InputStream
      implements Seekable, PositionedReadable {
    private final byte[] data;
    private final AtomicInteger failuresToInject;
    private final int failAtPosition;
    private final List<Long> readPositions;
    private long pos;

    FlakyPositionedStream(
        byte[] data, AtomicInteger failuresToInject, int failAtPosition, List<Long> readPositions) {
      this.data = data;
      this.failuresToInject = failuresToInject;
      this.failAtPosition = failAtPosition;
      this.readPositions = readPositions;
    }

    @Override
    public synchronized int read(long position, byte[] buffer, int offset, int length)
        throws IOException {
      readPositions.add(position);
      int limit = failuresToInject.get() > 0 ? failAtPosition : data.length;
      if (position >= limit) {
        failuresToInject.decrementAndGet();
        throw new IOException("Injected read failure");
      }
      int read = (int) Math.min(length, limit - position);
      System.arraycopy(data, (int) position, buffer, offset, read);
      return read;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      int read = 0;
      while (read < length) {
        int n = read(position + read, buffer, offset + read, length - read);
        if (n < 0) {
          throw new EOFException();
        }
        read += n;
      }
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      readFully(position, buffer, 0, buffer.length);
    }

    @Override
    public int read() {
      return pos < data.length ? data[(int) pos++] & 0xff : -1;
    }

    @Override
    public void seek(long pos) {
      this.pos = pos;
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public boolean seekToNewSource(long targetPos) {
      return false;
    }
  }
}
