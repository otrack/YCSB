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

import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;

import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A load balancing policy that uses LOCAL nodes first, then falls back to
 * REMOTE nodes when no LOCAL nodes are responsive.
 *
 * <p>All nodes present at session initialization are marked LOCAL.
 * Nodes added dynamically afterwards are marked REMOTE (fallback).
 * Query plans return LOCAL nodes (shuffled) followed by REMOTE nodes (shuffled).
 */
public class LocalFirstLoadBalancingPolicy implements LoadBalancingPolicy {

  private volatile DistanceReporter distanceReporter;
  private final CopyOnWriteArrayList<Node> localNodes = new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<Node> remoteNodes = new CopyOnWriteArrayList<>();

  /**
   * Required constructor for driver v4 policies loaded via configuration.
   * Actual initialization happens in {@link #init(Map, DistanceReporter)}.
   */
  public LocalFirstLoadBalancingPolicy(DriverContext context, String profileName) {
  }

  @Override
  public void init(Map<UUID, Node> nodes, DistanceReporter reporter) {
    this.distanceReporter = reporter;
    for (Node node : nodes.values()) {
      reporter.setDistance(node, NodeDistance.LOCAL);
      localNodes.add(node);
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
      // Nodes discovered after init are REMOTE (used as fallback)
      distanceReporter.setDistance(node, NodeDistance.REMOTE);
      remoteNodes.add(node);
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
}
