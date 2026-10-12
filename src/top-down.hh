// Finds which nodes of a graph a store lacks, without querying all of them.
//
// A store that holds a node also holds everything below it, so:
//   1. Query the roots.
//   2. Query the children of the nodes the store lacks.
//   3. Repeat until no node is left to query.
#pragma once

#include <algorithm>
#include <cstddef>
#include <set>
#include <utility>
#include <vector>

namespace nixgrpc {

// How many nodes one call may cover. Besides the current level, a call also
// takes the levels below it while they fit in the budget.
//
// A bigger budget means fewer round trips, but more nodes the store may
// already hold. Those are slow to answer.
inline constexpr std::size_t topDownBudget = 64;

// keys(n):     what the store must hold for node n
// children(n): the nodes below n
// query(keys): the subset of keys that the store holds
//
// Returns the reached nodes that the store lacks. A node without keys counts
// as lacking.
template <class Node, class Keys, class Children, class Query> class TopDownWalk
{
    using Key = decltype(std::declval<Keys &>()(std::declval<Node>()))::value_type;

    Keys keys;
    Children children;
    Query query;
    std::size_t budget;
    std::set<Key> valid;
    std::set<Node> queried;
    std::set<Node> seen;
    std::set<Node> missing;

    // The nodes of `level` not queried yet, then the levels below them while
    // the batch stays within the budget.
    auto nextBatch(const std::vector<Node> & level) -> std::vector<Node>
    {
        std::vector<Node> batch;
        for (const auto & node : level) {
            if (queried.insert(node).second) {
                batch.push_back(node);
            }
        }
        auto current = level;
        while (batch.size() < budget) {
            std::set<Node> below;
            for (const auto & node : current) {
                for (auto & child : children(node)) {
                    if (!queried.contains(child)) {
                        below.insert(std::move(child));
                    }
                }
            }
            if (below.empty() || batch.size() + below.size() > budget) {
                break;
            }
            queried.insert(below.begin(), below.end());
            batch.insert(batch.end(), below.begin(), below.end());
            current.assign(below.begin(), below.end());
        }
        return batch;
    }

    void queryBatch(const std::vector<Node> & batch)
    {
        std::set<Key> wanted;
        for (const auto & node : batch) {
            for (auto & key : keys(node)) {
                wanted.insert(std::move(key));
            }
        }
        if (!wanted.empty()) {
            valid.merge(query(wanted));
        }
    }

    auto isHeld(const Node & node) -> bool
    {
        auto const wanted = keys(node);
        return !wanted.empty()
            && std::ranges::all_of(wanted, [&](const Key & key) -> bool { return valid.contains(key); });
    }

public:
    TopDownWalk(Keys keys, Children children, Query query, std::size_t budget)
        : keys(std::move(keys))
        , children(std::move(children))
        , query(std::move(query))
        , budget(budget)
    {}

    auto run(std::vector<Node> level) -> std::set<Node>
    {
        while (true) {
            std::erase_if(level, [&](const Node & node) -> bool { return !seen.insert(node).second; });
            if (level.empty()) {
                return std::move(missing);
            }
            queryBatch(nextBatch(level));
            std::vector<Node> next;
            for (const auto & node : level) {
                if (!isHeld(node)) {
                    missing.insert(node);
                    for (auto & child : children(node)) {
                        next.push_back(std::move(child));
                    }
                }
            }
            level = std::move(next);
        }
    }
};

template <class Node, class Keys, class Children, class Query>
auto missingTopDown(std::vector<Node> roots, Keys keys, Children children, Query query,
                    std::size_t budget = topDownBudget) -> std::set<Node>
{
    return TopDownWalk<Node, Keys, Children, Query>(
               std::move(keys), std::move(children), std::move(query), budget)
        .run(std::move(roots));
}

} // namespace nixgrpc
