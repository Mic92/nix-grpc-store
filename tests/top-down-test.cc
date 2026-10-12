#include <cassert>
#include <cstddef>
#include <exception>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "top-down.hh"

namespace {

using Graph = std::map<std::string, std::vector<std::string>>;
using Calls = std::vector<std::vector<std::string>>;

struct Result
{
    std::set<std::string> missing;
    Calls calls;
};

auto run(const Graph & graph, std::vector<std::string> roots, const std::set<std::string> & held, std::size_t budget) -> Result
{
    Result result;
    result.missing = nixgrpc::missingTopDown(
        std::move(roots),
        [](const std::string & node) -> std::vector<std::string> { return {node + "-out"}; },
        [&](const std::string & node) -> std::vector<std::string> { return graph.at(node); },
        [&](const std::set<std::string> & keys) -> std::set<std::string> {
            result.calls.emplace_back(keys.begin(), keys.end());
            std::set<std::string> answer;
            for (const auto & key : keys) {
                if (held.contains(key.substr(0, key.size() - 4))) {
                    answer.insert(key);
                }
            }
            return answer;
        },
        budget);
    return result;
}

auto chain() -> Graph
{
    return {{"root", {"a"}}, {"a", {"b"}}, {"b", {"c"}}, {"c", {"d"}}, {"d", {}}};
}

using Names = std::set<std::string>;

void aHeldRootIsQueriedAlone()
{
    auto const result = run(chain(), {"root"}, {"root", "a", "b", "c", "d"}, 1);
    assert(result.missing.empty());
    assert((result.calls == Calls{{"root-out"}}));
}

void aHeldInputStopsTheWalk()
{
    auto const result = run(chain(), {"root"}, {"a", "b", "c", "d"}, 1);
    assert((result.missing == Names{"root"}));
    assert((result.calls == Calls{{"root-out"}, {"a-out"}}));
}

void aColdStoreIsWalkedALevelAtATime()
{
    auto const result = run(chain(), {"root"}, {}, 1);
    assert((result.missing == Names{"root", "a", "b", "c", "d"}));
    assert((result.calls == Calls{{"root-out"}, {"a-out"}, {"b-out"}, {"c-out"}, {"d-out"}}));
}

void aSharedInputIsQueriedOnce()
{
    Graph const diamond{{"x", {"a", "b"}}, {"a", {"s"}}, {"b", {"s"}}, {"s", {}}};
    auto const result = run(diamond, {"x"}, {}, 1);
    assert((result.missing == Names{"x", "a", "b", "s"}));
    assert((result.calls == Calls{{"x-out"}, {"a-out", "b-out"}, {"s-out"}}));
}

void aCallWidensWhileItFitsTheBudget()
{
    auto const result = run(chain(), {"root"}, {}, 3);
    assert((result.missing == Names{"root", "a", "b", "c", "d"}));
    assert((result.calls == Calls{{"a-out", "b-out", "root-out"}, {"c-out", "d-out"}}));
}

void aWideLevelIsNotSplit()
{
    Graph const fan{{"x", {"a", "b", "c"}}, {"a", {}}, {"b", {}}, {"c", {}}};
    auto const result = run(fan, {"x"}, {}, 3);
    assert((result.missing == Names{"x", "a", "b", "c"}));
    assert((result.calls == Calls{{"x-out"}, {"a-out", "b-out", "c-out"}}));
}

void aHeldNodeBelowAMissingOneIsFoundByAWideCall()
{
    auto const result = run(chain(), {"root"}, {"c", "d"}, 64);
    assert((result.missing == Names{"root", "a", "b"}));
    assert((result.calls == Calls{{"a-out", "b-out", "c-out", "d-out", "root-out"}}));
}

void aNodeWithoutKeysIsMissingAndItsChildrenAreWalked()
{
    Graph const graph{{"top", {"in"}}, {"in", {}}};
    std::vector<std::string> queried;
    auto const missing = nixgrpc::missingTopDown(
        std::vector<std::string>{"top"},
        [](const std::string & node) -> std::vector<std::string> {
            return node == "top" ? std::vector<std::string>{} : std::vector<std::string>{node};
        },
        [&](const std::string & node) -> std::vector<std::string> { return graph.at(node); },
        [&](const std::set<std::string> & keys) -> std::set<std::string> {
            queried.insert(queried.end(), keys.begin(), keys.end());
            return keys;
        },
        1);
    assert((missing == Names{"top"}));
    assert((queried == std::vector<std::string>{"in"}));
}

void aNodeWithoutKeysIsMissing()
{
    auto const missing = nixgrpc::missingTopDown(
        std::vector<int>{1},
        [](int) -> std::vector<int> { return {}; },
        [](int) -> std::vector<int> { return {}; },
        [](const std::set<int> &) -> std::set<int> {
            assert(false);
            return {};
        });
    assert((missing == std::set<int>{1}));
}

} // namespace

auto main() -> int
try {
    aHeldRootIsQueriedAlone();
    aHeldInputStopsTheWalk();
    aColdStoreIsWalkedALevelAtATime();
    aSharedInputIsQueriedOnce();
    aCallWidensWhileItFitsTheBudget();
    aWideLevelIsNotSplit();
    aHeldNodeBelowAMissingOneIsFoundByAWideCall();
    aNodeWithoutKeysIsMissing();
    aNodeWithoutKeysIsMissingAndItsChildrenAreWalked();
    return 0;
} catch (const std::exception & err) {
    std::cerr << "top-down-test: " << err.what() << '\n';
    return 1;
}
