/*
 * Copyright (c) 2019 the Eclipse Milo Authors
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 */

package org.eclipse.milo.examples.client;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.nodes.UaNode;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrowseAsyncExample implements ClientExample {

    public static void main(String[] args) throws Exception {
        BrowseAsyncExample example = new BrowseAsyncExample();

        new ClientExampleRunner(example, false).run();
    }

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void run(OpcUaClient client, CompletableFuture<OpcUaClient> future) throws Exception {
        // synchronous connect
        client.connect().get();

        // start browsing at root folder
        UaNode rootNode = client.getAddressSpace().getNode(Identifiers.RootFolder);

        Tree<UaNode> tree = new Tree<>(rootNode);

        long startTime = System.nanoTime();
        browseRecursive(client, tree).get();
        long endTime = System.nanoTime();

        traverse(tree, 0, (depth, n) -> logger.info(indent(depth) + n.getBrowseName().getName()));

        logger.info(
            "Browse took {}ms",
            TimeUnit.MILLISECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS));

        future.complete(client);
    }

    private CompletableFuture<Void> browseRecursive(OpcUaClient client, Tree<UaNode> tree) {
        return client.getAddressSpace().browseNodesAsync(tree.node).thenCompose(nodes -> {
            // Add each child node to the tree
            nodes.forEach(tree::addChild);

            // For each child node browse for its children
            Stream<CompletableFuture<Void>> futures =
                tree.children.stream().map(child -> browseRecursive(client, child));

            // Return a CompletableFuture that completes when the child browses complete
            return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
        });
    }

    private static String indent(int depth) {
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < depth; i++) {
            s.append("  ");
        }
        return s.toString();
    }

    private static <T> void traverse(Tree<T> tree, int depth, BiConsumer<Integer, T> consumer) {
        consumer.accept(depth, tree.node);

        tree.children.forEach(child -> traverse(child, depth + 1, consumer));
    }

    @Override
    public String getEndpointUrl(){
        return "opc.tcp://127.0.0.1:4840/freeopcua/server/";
    }

    private static class Tree<T> {

        final List<Tree<T>> children = Lists.newCopyOnWriteArrayList();

        final T node;

        Tree(T node) {
            this.node = node;
        }

        void addChild(T child) {
            children.add(new Tree<>(child));
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("node", node)
                .add("children", children)
                .toString();
        }

    }

}
