/*
 * Copyright 2017 TiKV Project Authors.
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

package org.tikv.common;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import java.net.ServerSocket;
import java.util.Arrays;
import org.tikv.common.codec.Codec.BytesCodec;
import org.tikv.common.codec.CodecDataOutput;
import org.tikv.kvproto.Metapb.Peer;
import org.tikv.kvproto.Metapb.Region;
import org.tikv.kvproto.Metapb.RegionEpoch;
import org.tikv.kvproto.Metapb.Store;
import org.tikv.kvproto.Metapb.StoreLabel;
import org.tikv.kvproto.Metapb.StoreState;
import org.tikv.kvproto.Pdpb.*;

public class GrpcUtils {
  private static ResponseHeader makeDefaultHeader(long clusterId) {
    return ResponseHeader.newBuilder().setClusterId(clusterId).build();
  }

  public static Member makeMember(long memberId, String... urls) {
    return Member.newBuilder().setMemberId(memberId).addAllClientUrls(Arrays.asList(urls)).build();
  }

  public static GetMembersResponse makeGetMembersResponse(long clusterId, Member... members) {
    return GetMembersResponse.newBuilder()
        .setHeader(makeDefaultHeader(clusterId))
        .setLeader(members[0])
        .addAllMembers(Arrays.asList(members))
        .build();
  }

  public static TsoResponse makeTsoResponse(long clusterId, long physical, long logical) {
    Timestamp ts = Timestamp.newBuilder().setPhysical(physical).setLogical(logical).build();
    return TsoResponse.newBuilder()
        .setHeader(makeDefaultHeader(clusterId))
        .setCount(1)
        .setTimestamp(ts)
        .build();
  }

  public static Peer makePeer(long id, long storeId) {
    return Peer.newBuilder().setStoreId(storeId).setId(id).build();
  }

  public static ByteString encodeKey(byte[] key) {
    CodecDataOutput cdo = new CodecDataOutput();
    BytesCodec.writeBytes(cdo, key);
    return cdo.toByteString();
  }

  public static RegionEpoch makeRegionEpoch(long confVer, long ver) {
    return RegionEpoch.newBuilder().setConfVer(confVer).setVersion(ver).build();
  }

  public static Region makeRegion(
      long id, ByteString startKey, ByteString endKey, RegionEpoch re, Peer... peers) {
    return Region.newBuilder()
        .setId(id)
        .setStartKey(startKey)
        .setEndKey(endKey)
        .setRegionEpoch(re)
        .addAllPeers(Lists.newArrayList(peers))
        .build();
  }

  public static GetRegionResponse makeGetRegionResponse(long clusterId, Region region) {
    return GetRegionResponse.newBuilder()
        .setHeader(makeDefaultHeader(clusterId))
        .setRegion(region)
        .setLeader(region.getPeers(0))
        .build();
  }

  public static StoreLabel makeStoreLabel(String key, String value) {
    return StoreLabel.newBuilder().setKey(key).setValue(value).build();
  }

  public static Store makeStore(long id, String address, StoreState state, StoreLabel... labels) {
    return Store.newBuilder()
        .setId(id)
        .setAddress(address)
        .setState(state)
        .addAllLabels(Arrays.asList(labels))
        .build();
  }

  public static GetStoreResponse makeGetStoreResponse(long clusterId, Store store) {
    return GetStoreResponse.newBuilder()
        .setHeader(makeDefaultHeader(clusterId))
        .setStore(store)
        .build();
  }

  public static int getFreePort() {
    while (true) {
      try (ServerSocket s = new ServerSocket(0)) {
        return s.getLocalPort();
      } catch (Exception ignore) {
      }
    }
  }
}
