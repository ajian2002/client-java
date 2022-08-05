/*
 * Copyright 2022 TiKV Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.tikv.txn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.BaseTxnKVTest;
import org.tikv.common.Snapshot;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.region.RegionStoreClient.RegionStoreClientBuilder;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.kvproto.Kvrpcpb.KvPair;
import org.tikv.util.TestUtils;

public class TwoPhaseCommitterTest extends BaseTxnKVTest {
  private static final int WRITE_BUFFER_SIZE = 32 * 1024;
  private static final int TXN_COMMIT_BATCH_SIZE = 768 * 1024;
  private static final long DEFAULT_BATCH_WRITE_LOCK_TTL = 3600000;
  private static final Logger logger = LoggerFactory.getLogger(TwoPhaseCommitterTest.class);

  private RegionStoreClientBuilder clientBuilder;
  private TiSession session;
  private TxnKVClient txnKVClient;
  private Long lockTTLSeconds = 20L;

  @Before
  public void setUp() {
    TiConfiguration conf = createTiConfiguration();
    try {
      session = TiSession.create(conf);
      clientBuilder = session.getRegionStoreClientBuilder();
      txnKVClient = session.createTxnClient();
    } catch (Exception e) {
      fail("TiDB cluster may not be present");
    }
  }

  @After
  public void tearDown() throws Exception {
    if (session != null) {
      session.close();
    }
  }

  @Test
  public void autoClosableTest() throws Exception {
    long startTS = session.getTimestamp().getVersion();
    ExecutorService executorService =
        Executors.newFixedThreadPool(
            WRITE_BUFFER_SIZE,
            new ThreadFactoryBuilder().setNameFormat("2pc-pool-%d").setDaemon(true).build());
    Assert.assertFalse(executorService.isShutdown());
    try (TwoPhaseCommitter twoPhaseCommitter =
        new TwoPhaseCommitter(
            session,
            startTS,
            DEFAULT_BATCH_WRITE_LOCK_TTL,
            TXN_COMMIT_BATCH_SIZE,
            TXN_COMMIT_BATCH_SIZE,
            WRITE_BUFFER_SIZE,
            1,
            true,
            3,
            executorService)) {}
    Assert.assertTrue(executorService.isShutdown());
  }

  @Test
  public void batchGetRetryTest() throws Exception {
    byte[] primaryKey = "key1".getBytes(StandardCharsets.UTF_8);
    byte[] key2 = "key2".getBytes(StandardCharsets.UTF_8);
    ByteString bkey1 = ByteString.copyFromUtf8("key1");
    ByteString bvalue1 = ByteString.copyFromUtf8("val1");
    ByteString bvalue2 = ByteString.copyFromUtf8("val2");
    ByteString bvalue3 = ByteString.copyFromUtf8("val3");
    try (KVClient kvClient = session.createKVClient()) {
      session.createRawClient().batchDelete(Collections.singletonList(bkey1));
      new Thread(
              () -> {
                BackOffer aBackOffer = ConcreteBackOffer.newCustomBackOff(60000);
                Snapshot snapshot = session.createSnapshot(session.getTimestamp());
                try (TwoPhaseCommitter twoPhaseCommitter =
                    new TwoPhaseCommitter(session, snapshot.getVersion())) {
                  // first phrase: prewrite
                  twoPhaseCommitter.prewritePrimaryKey(
                      aBackOffer, primaryKey, "val1".getBytes(StandardCharsets.UTF_8));

                  // second phrase: commit
                  long commitTS = session.getTimestamp().getVersion();
                  Thread.sleep(10000L);
                  twoPhaseCommitter.commitPrimaryKey(aBackOffer, primaryKey, commitTS);

                  ByteString val = kvClient.get(bkey1, snapshot.getVersion());
                  assertEquals(bvalue1, val);
                  logger.info("assert 1 pass");
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              })
          .start();

      Thread.sleep(3000L);
      Snapshot snapshot = session.createSnapshot(session.getTimestamp());
      try (TwoPhaseCommitter twoPhaseCommitter =
          new TwoPhaseCommitter(session, snapshot.getVersion())) {
        BackOffer bBackOffer = ConcreteBackOffer.newCustomBackOff(30000);
        kvClient.batchGet(bBackOffer, Collections.singletonList(bkey1), snapshot.getVersion());

        List<KvPair> kvPairs =
            kvClient.batchGet(bBackOffer, Collections.singletonList(bkey1), snapshot.getVersion());

        // first phrase: prewrite
        twoPhaseCommitter.prewritePrimaryKey(bBackOffer, primaryKey, bvalue3.toByteArray());
        // second phrase: commit
        long commitTS = session.getTimestamp().getVersion();
        twoPhaseCommitter.commitPrimaryKey(bBackOffer, primaryKey, commitTS);
        logger.info("assert 2 pass");
      }

      BackOffer cBackOffer = ConcreteBackOffer.newCustomBackOff(3000);
      List<KvPair> kvPairs =
          kvClient.batchGet(cBackOffer, Collections.singletonList(bkey1), snapshot.getVersion());
      assertEquals(1, kvPairs.size());
      logger.info("assert 3 pass");
    }
  }

  @Test
  public void PCTest() throws Exception {
    String key1 = String.valueOf(TestUtils.genRandomKey("key", 7));
    String key2 = "key2";
    String value1 = "value1";
    String value2 = "value2";
    new Thread(
            () -> {
              long startTs = session.getTimestamp().getVersion();
              TwoPhaseCommitter twoPhaseCommitter = new TwoPhaseCommitter(session, startTs);
              BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(30000);
              try {
                System.out.println("1:start to prewrite");
                twoPhaseCommitter.prewritePrimaryKey(
                    backOffer, key1.getBytes("UTF-8"), value1.getBytes("UTF-8"));
              } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
              }

              long commitTs = session.getTimestamp().getVersion();
              try {
                Thread.sleep(5000);
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
              System.out.println("1:start to commit");
              twoPhaseCommitter.commitPrimaryKey(
                  backOffer, key1.getBytes(StandardCharsets.UTF_8), commitTs);
              try {
                twoPhaseCommitter.close();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            })
        .start();

    Thread.sleep(2000);
    long startTs = session.getTimestamp().getVersion();
    TwoPhaseCommitter twoPhaseCommitter = new TwoPhaseCommitter(session, startTs);
    BackOffer backOffer = ConcreteBackOffer.newCustomBackOff(60000);
    //
    KVClient kvClient = session.createKVClient();
    Snapshot snapshot = session.createSnapshot(session.getTimestamp());
    List<KvPair> kvPairs =
        kvClient.batchGet(
            backOffer,
            Collections.singletonList(ByteString.copyFromUtf8(key1)),
            snapshot.getVersion());

    System.out.println("2:start to prewrite");
    twoPhaseCommitter.prewritePrimaryKey(
        backOffer, key1.getBytes("UTF-8"), value2.getBytes("UTF-8"));

    System.out.println("2:start to commit");
    long commitTs = session.getTimestamp().getVersion();
    twoPhaseCommitter.commitPrimaryKey(backOffer, key1.getBytes("UTF-8"), commitTs);
    twoPhaseCommitter.close();

    assertEquals(value2, new String(session.createSnapshot().get(key1.getBytes("UTF-8")), "UTF-8"));
  }
}
