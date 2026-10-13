/**
 * Copyright (c) 2026 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 */
package site.ycsb.db;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy.DistanceReporter;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * Query plans of {@link LocalFirstLoadBalancingPolicy}: local nodes before remote ones, and the replicas of the
 * routing key first in each group. The driver's interfaces are faked with dynamic proxies.
 */
public class LocalFirstLoadBalancingPolicyTest {

  private static final CqlIdentifier KEYSPACE = CqlIdentifier.fromCql("ycsb");
  private static final ByteBuffer KEY = ByteBuffer.wrap(new byte[]{1, 2, 3});

  /** A proxy of {@code type} answering the methods of {@code answers} by name, and null otherwise. */
  @SuppressWarnings("unchecked")
  private static <T> T fake(Class<T> type, Map<String, Object> answers) {
    return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (proxy, method, args) -> {
        switch (method.getName()) {
        case "equals":
          return proxy == args[0];
        case "hashCode":
          return System.identityHashCode(proxy);
        case "toString":
          return type.getSimpleName() + answers;
        default:
          return answers.get(method.getName());
        }
      });
  }

  private static Map<String, Object> answers(Object... kv) {
    Map<String, Object> m = new LinkedHashMap<>();
    for (int i = 0; i < kv.length; i += 2) {
      m.put((String) kv[i], kv[i + 1]);
    }
    return m;
  }

  private static Node node(String ip, String dc) {
    InetSocketAddress address = new InetSocketAddress(ip, 9042);
    EndPoint endPoint = fake(EndPoint.class, answers("resolve", address));
    return fake(Node.class, answers("getEndPoint", endPoint, "getDatacenter", dc, "getHostId", UUID.randomUUID()));
  }

  private static LocalFirstLoadBalancingPolicy policy(String contactPoint, List<Node> nodes) {
    DriverExecutionProfile profile = fake(DriverExecutionProfile.class,
        answers("getStringList", Collections.singletonList(contactPoint + ":9042")));
    DriverConfig config = fake(DriverConfig.class, answers("getDefaultProfile", profile));
    DriverContext context = fake(DriverContext.class, answers("getConfig", config));
    LocalFirstLoadBalancingPolicy policy = new LocalFirstLoadBalancingPolicy(context, "default");
    Map<UUID, Node> byId = new LinkedHashMap<>();
    for (Node n : nodes) {
      byId.put(n.getHostId(), n);
    }
    policy.init(byId, fake(DistanceReporter.class, answers()));
    return policy;
  }

  private static Session session(Set<Node> replicasOfKey) {
    TokenMap tokenMap = (TokenMap) Proxy.newProxyInstance(TokenMap.class.getClassLoader(),
        new Class<?>[]{TokenMap.class}, (proxy, method, args) -> {
          if (method.getName().equals("getReplicas") && args.length == 2
              && KEYSPACE.equals(args[0]) && KEY.equals(args[1])) {
            return replicasOfKey;
          }
          return method.getName().equals("getReplicas") ? Collections.emptySet() : null;
        });
    Metadata metadata = fake(Metadata.class, answers("getTokenMap", Optional.of(tokenMap)));
    return fake(Session.class, answers("getMetadata", metadata, "getKeyspace", Optional.of(KEYSPACE)));
  }

  private static List<Node> plan(LocalFirstLoadBalancingPolicy policy, Request request, Session session) {
    return new ArrayList<>(policy.newQueryPlan(request, session));
  }

  @Test
  public void replicasOfTheRoutingKeyComeFirstInEachDatacenter() {
    Node a1 = node("10.0.0.1", "A");
    Node a2 = node("10.0.0.2", "A");
    Node b1 = node("10.0.1.1", "B");
    Node b2 = node("10.0.1.2", "B");
    LocalFirstLoadBalancingPolicy policy = policy("10.0.0.1", Arrays.asList(a1, a2, b1, b2));
    Session session = session(new HashSet<>(Arrays.asList(a2, b2)));
    Request request = fake(Request.class, answers("getRoutingKey", KEY));

    for (int i = 0; i < 20; i++) {
      List<Node> plan = plan(policy, request, session);
      assertEquals(4, plan.size());
      assertEquals(a2, plan.get(0));
      assertEquals(a1, plan.get(1));
      assertEquals(b2, plan.get(2));
      assertEquals(b1, plan.get(3));
    }
  }

  @Test
  public void localNodesComeFirstWithoutRoutingKey() {
    Node a1 = node("10.0.0.1", "A");
    Node a2 = node("10.0.0.2", "A");
    Node b1 = node("10.0.1.1", "B");
    LocalFirstLoadBalancingPolicy policy = policy("10.0.0.1", Arrays.asList(a1, a2, b1));
    Session session = session(Collections.singleton(b1));

    for (Request request : Arrays.asList(null, fake(Request.class, answers()))) {
      List<Node> plan = plan(policy, request, session);
      assertEquals(new HashSet<>(Arrays.asList(a1, a2)), new HashSet<>(plan.subList(0, 2)));
      assertEquals(b1, plan.get(2));
    }
  }
}
