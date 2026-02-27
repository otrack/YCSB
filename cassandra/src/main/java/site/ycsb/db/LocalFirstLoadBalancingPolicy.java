/**
 * Copyright (c) 2013-2015 YCSB contributors. All rights reserved.
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

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A load balancing policy that uses LOCAL datacenter nodes first, then falls
 * back to REMOTE datacenter nodes when no LOCAL nodes are responsive.
 *
 * <p>The local datacenter is detected automatically at session initialization:
 * the contact-point hosts (from {@code HOSTS_PROPERTY}) are used as seeds to
 * identify the local datacenter. All other nodes discovered via gossip that
 * belong to a different datacenter are classified as REMOTE and used only as
 * a fallback.
 *
 * <p>Query plans return LOCAL nodes (shuffled) followed by REMOTE nodes
 * (shuffled).
 */
public class LocalFirstLoadBalancingPolicy implements LoadBalancingPolicy {

  private volatile DistanceReporter distanceReporter;
  private final CopyOnWriteArrayList<Node> localNodes = new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<Node> remoteNodes = new CopyOnWriteArrayList<>();

  /**
   * Host strings of the configured contact points (local DC seeds), as read
   * from {@link DefaultDriverOption#CONTACT_POINTS} in the driver config.
   * Each entry is a bare hostname or IP (no port).
   */
  private final Set<String> contactPointHosts;

  /** The datacenter determined to be local, detected during {@link #init}. */
  private volatile String localDc;

  /**
   * Required constructor for driver v4 policies loaded via configuration.
   * The contact-point host strings are captured here so that the local
   * datacenter can be determined during {@link #init(Map, DistanceReporter)}.
   */
  public LocalFirstLoadBalancingPolicy(DriverContext context, String profileName) {
    Set<String> hosts = new HashSet<>();
    try {
      List<String> cpStrings = context.getConfig()
          .getDefaultProfile()
          .getStringList(DefaultDriverOption.CONTACT_POINTS);
      for (String cp : cpStrings) {
        // Contact points are stored as "host:port".
        // Handle IPv6 bracket notation: "[::1]:9042" → "::1".
        String h;
        if (cp.startsWith("[")) {
          int closeBracket = cp.indexOf(']');
          h = closeBracket >= 0 ? cp.substring(1, closeBracket).trim() : cp.trim();
        } else {
          int lastColon = cp.lastIndexOf(':');
          h = lastColon >= 0 ? cp.substring(0, lastColon).trim() : cp.trim();
        }
        hosts.add(h);
      }
    } catch (IllegalStateException ignored) {
      // CONTACT_POINTS not set in config; localDc detection will fall back to
      // treating all nodes as local (preserves backward-compatible behaviour).
    }
    contactPointHosts = Collections.unmodifiableSet(hosts);
  }

  @Override
  public void init(Map<UUID, Node> nodes, DistanceReporter reporter) {
    this.distanceReporter = reporter;

    // Determine the local DC: find a node whose address matches a contact point.
    String detectedDc = null;
    if (!contactPointHosts.isEmpty()) {
      for (Node node : nodes.values()) {
        SocketAddress sa = node.getEndPoint().resolve();
        if (sa instanceof InetSocketAddress) {
          InetSocketAddress isa = (InetSocketAddress) sa;
          String nodeIp = isa.getAddress().getHostAddress();
          String nodeHost = isa.getHostString();
          if ((contactPointHosts.contains(nodeIp) || contactPointHosts.contains(nodeHost))
              && node.getDatacenter() != null) {
            detectedDc = node.getDatacenter();
            break;
          }
        }
      }
    }
    localDc = detectedDc;

    // Classify every known node: same DC -> LOCAL, other DC -> REMOTE.
    for (Node node : nodes.values()) {
      if (isLocal(node)) {
        reporter.setDistance(node, NodeDistance.LOCAL);
        localNodes.add(node);
      } else {
        reporter.setDistance(node, NodeDistance.REMOTE);
        remoteNodes.add(node);
      }
    }
  }

  @Override
  public Queue<Node> newQueryPlan(Request request, Session session) {
    // LOCAL nodes first (random order), then REMOTE nodes (random order)
    List<Node> local = new ArrayList<>(localNodes);
    Collections.shuffle(local);
    List<Node> remote = new ArrayList<>(remoteNodes);
    Collections.shuffle(remote);
    ArrayDeque<Node> plan = new ArrayDeque<>(local.size() + remote.size());
    plan.addAll(local);
    plan.addAll(remote);
    return plan;
  }

  @Override
  public void onAdd(Node node) {
    if (!localNodes.contains(node) && !remoteNodes.contains(node)) {
      if (isLocal(node)) {
        distanceReporter.setDistance(node, NodeDistance.LOCAL);
        localNodes.add(node);
      } else {
        distanceReporter.setDistance(node, NodeDistance.REMOTE);
        remoteNodes.add(node);
      }
    }
  }

  @Override
  public void onUp(Node node) {
  }

  @Override
  public void onDown(Node node) {
  }

  @Override
  public void onRemove(Node node) {
    localNodes.remove(node);
    remoteNodes.remove(node);
  }

  @Override
  public void close() {
  }

  /**
   * Returns {@code true} if {@code node} belongs to the local datacenter.
   * When no local DC has been detected (e.g. single-DC cluster or no
   * contact-point match), all nodes are treated as local.
   */
  private boolean isLocal(Node node) {
    return localDc == null || localDc.equals(node.getDatacenter());
  }
}
