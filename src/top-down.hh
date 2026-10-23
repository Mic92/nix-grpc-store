// Finds which nodes of a graph a store lacks, without querying all of them.
//
// A store that holds a node also holds everything below it, so:
//   1. Query the roots.
//   2. Query the children of the nodes the store lacks.
//   3. Repeat until no node is left to query.
#pragma once

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <map>
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

// The nodes of `level` not queried yet, then the levels below them while the
// batch stays within the budget.
template <class Node, class Children>
auto nextBatch(const std::vector<Node> & level, Children & children, std::set<Node> & queried, std::size_t budget)
    -> std::vector<Node>
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
            queryBatch(nextBatch(level, children, queried, budget));
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

// For a build. roots maps each requested node to the keys asked of it, and
// uses(n) lists the nodes n builds on, each with the keys n needs of it.
//
// As in nix, a node is needed if it is requested or a node that is built uses
// it, and it needs the keys asked of it and those such nodes use. So a node is
// decided only once every node that uses it is, and one used only by held
// nodes is not needed. As nix substitutes before it builds, fetch(keys) gets
// what a needed node lacks before the node counts as lacking. With
// rebuildRoots (--repair, --check) the roots count as lacking whatever the
// store holds.
template <class Node, class Key, class Uses, class Query, class Fetch> class BuildWalk
{
    Query query;
    Fetch fetch;
    std::size_t budget;
    std::set<Node> rebuilt;
    // The graph below the roots, and how many nodes use each node.
    std::map<Node, decltype(std::declval<Uses &>()(std::declval<const Node &>()))> used;
    std::map<Node, std::size_t> users;
    // What a request or a node that is built needs of a node, and what any
    // node asks of it, which the calls query ahead.
    std::map<Node, std::set<Key>> needs;
    std::map<Node, std::set<Key>> anyNeeds;
    std::vector<Node> level;
    std::set<Key> valid;
    std::set<Node> queried;
    std::set<Node> missing;

    auto isHeld(const Node & node) -> bool
    {
        auto const & keys = needs.at(node);
        return !rebuilt.contains(node) && !keys.empty() && std::ranges::includes(valid, keys);
    }

    void queryAhead(const std::vector<Node> & needed)
    {
        auto children = [&](const Node & node) -> std::vector<Node> {
            std::vector<Node> below;
            for (const auto & [child, keys] : used.at(node)) {
                below.push_back(child);
            }
            return below;
        };
        std::set<Key> keys;
        for (const auto & node : nextBatch(needed, children, queried, budget)) {
            keys.insert(anyNeeds[node].begin(), anyNeeds[node].end());
        }
        if (!keys.empty()) {
            valid.merge(query(keys));
        }
    }

    void fetchLacking(const std::vector<Node> & needed)
    {
        std::set<Key> lacking;
        for (const auto & node : needed) {
            if (!rebuilt.contains(node)) {
                std::ranges::set_difference(needs.at(node), valid, std::inserter(lacking, lacking.end()));
            }
        }
        if (!lacking.empty()) {
            valid.merge(fetch(lacking));
        }
    }

    // Decides the nodes of the level, and returns the nodes whose users are
    // all decided now.
    auto decide() -> std::vector<Node>
    {
        std::vector<Node> next;
        for (const auto & node : level) {
            if (needs.contains(node) && !isHeld(node)) {
                missing.insert(node);
                for (const auto & [child, keys] : used.at(node)) {
                    needs[child].insert(keys.begin(), keys.end());
                }
            }
            for (const auto & [child, keys] : used.at(node)) {
                if (--users.at(child) == 0) {
                    next.push_back(child);
                }
            }
        }
        return next;
    }

public:
    BuildWalk(const std::map<Node, std::vector<Key>> & roots, bool rebuildRoots, Uses uses, Query query, Fetch fetch,
              std::size_t budget)
        : query(std::move(query))
        , fetch(std::move(fetch))
        , budget(budget)
    {
        std::vector<Node> todo;
        for (const auto & [root, wanted] : roots) {
            auto & keys = needs[root];
            if (rebuildRoots) {
                rebuilt.insert(root);
            } else {
                keys.insert(wanted.begin(), wanted.end());
            }
            anyNeeds[root] = keys;
            todo.push_back(root);
        }
        while (!todo.empty()) {
            auto node = std::move(todo.back());
            todo.pop_back();
            if (used.contains(node)) {
                continue;
            }
            for (const auto & [child, keys] : used.emplace(node, uses(node)).first->second) {
                anyNeeds[child].insert(keys.begin(), keys.end());
                ++users[child];
                todo.push_back(child);
            }
        }
        for (const auto & [root, wanted] : roots) {
            if (!users.contains(root)) {
                level.push_back(root);
            }
        }
    }

    auto run() -> std::set<Node>
    {
        while (!level.empty()) {
            std::vector<Node> needed;
            std::ranges::copy_if(level, std::back_inserter(needed),
                                 [&](const Node & node) -> bool { return needs.contains(node); });
            queryAhead(needed);
            fetchLacking(needed);
            level = decide();
        }
        return std::move(missing);
    }
};

template <class Node, class Key, class Uses, class Query, class Fetch>
auto missingForBuild(const std::map<Node, std::vector<Key>> & roots, bool rebuildRoots, Uses uses, Query query,
                     Fetch fetch, std::size_t budget = topDownBudget) -> std::set<Node>
{
    return BuildWalk<Node, Key, Uses, Query, Fetch>(roots, rebuildRoots, std::move(uses), std::move(query),
                                                    std::move(fetch), budget)
        .run();
}

} // namespace nixgrpc
