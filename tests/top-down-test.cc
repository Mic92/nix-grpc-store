#include <algorithm>
#include <cassert>
#include <cstddef>
#include <exception>
#include <iostream>
#include <iterator>
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

// Each node has the keys "-out" and "-man". Uses maps a node to the nodes it
// builds on, with the keys it needs of each; held lists keys.
using Uses = std::map<std::string, std::vector<std::pair<std::string, std::vector<std::string>>>>;

auto runBuild(const Uses & uses, const std::map<std::string, std::vector<std::string>> & roots, const Names & held,
              bool rebuildRoots) -> Result
{
    Result result;
    result.missing = nixgrpc::missingForBuild(
        roots, rebuildRoots, [&](const std::string & node) -> Uses::mapped_type { return uses.at(node); },
        [&](const std::set<std::string> & keys) -> std::set<std::string> {
            result.calls.emplace_back(keys.begin(), keys.end());
            Names answer;
            std::ranges::set_intersection(keys, held, std::inserter(answer, answer.end()));
            return answer;
        },
        [](const Names &) -> Names { return {}; }, 1);
    return result;
}

// b builds on a's out.
auto bOnA() -> Uses
{
    return {{"b", {{"a", {"a-out"}}}}, {"a", {}}};
}

void aRootNeedsOnlyTheKeysAskedOfIt()
{
    auto const result = runBuild(bOnA(), {{"b", {"b-out"}}}, {"b-out"}, false);
    assert(result.missing.empty());
    assert((result.calls == Calls{{"b-out"}}));
}

void aRebuiltRootIsMissingWhatTheStoreHolds()
{
    auto const result = runBuild(bOnA(), {{"b", {"b-out"}}}, {"b-out", "b-man", "a-out", "a-man"}, true);
    assert((result.missing == Names{"b"}));
    assert((result.calls == Calls{{"a-out"}}));
}

void anInputNeedsOnlyTheKeysItsDependantsUse()
{
    auto const result = runBuild(bOnA(), {{"b", {"b-out"}}}, {"a-out"}, false);
    assert((result.missing == Names{"b"}));
    assert((result.calls == Calls{{"b-out"}, {"a-out"}}));
}

void anInputNeedsTheKeysOfAllItsDependants()
{
    // x builds on a's out and c's out, c on b's out, b on a's man. b is
    // further down than a, so a is decided only after b.
    Uses const uses{{"x", {{"a", {"a-out"}}, {"c", {"c-out"}}}},
                    {"c", {{"b", {"b-out"}}}},
                    {"b", {{"a", {"a-man"}}}},
                    {"a", {}}};
    auto const result = runBuild(uses, {{"x", {"x-out"}}}, {"a-out"}, false);
    assert((result.missing == Names{"x", "c", "b", "a"}));
}

// b is held, so it is not built and a need not have the man it uses.
void anInputUsedOnlyByAHeldNodeIsNotNeeded()
{
    Uses const uses{{"x", {{"a", {"a-out"}}, {"b", {"b-out"}}}}, {"b", {{"a", {"a-man"}}}}, {"a", {}}};
    auto const result = runBuild(uses, {{"x", {"x-out"}}}, {"a-out", "b-out"}, false);
    assert((result.missing == Names{"x"}));
}

void aRootAnotherBuildsOnNeedsWhatBothAsk()
{
    auto const held = runBuild(bOnA(), {{"b", {"b-out"}}, {"a", {"a-out"}}}, {"a-out"}, false);
    assert((held.missing == Names{"b"}));
    auto const missing = runBuild(bOnA(), {{"b", {"b-out"}}, {"a", {"a-man"}}}, {"a-man"}, false);
    assert((missing.missing == Names{"a", "b"}));
}

// A store that holds valid and can fetch fetchable, like a server that
// substitutes what a trusted key signed. Records each call.
struct Fetching
{
    Names valid;
    Names fetchable;
    Calls queried;
    Calls fetched;

    auto plan(const std::map<std::string, std::vector<std::string>> & roots, std::size_t budget) -> Names
    {
        // t builds on x's out, x on w's out.
        Uses const uses{{"t", {{"x", {"x-out"}}}}, {"x", {{"w", {"w-out"}}}}, {"w", {}}};
        return nixgrpc::missingForBuild(
            roots, false, [&](const std::string & node) -> Uses::mapped_type { return uses.at(node); },
            [&](const Names & keys) -> Names {
                queried.emplace_back(keys.begin(), keys.end());
                Names answer;
                std::ranges::set_intersection(keys, valid, std::inserter(answer, answer.end()));
                return answer;
            },
            [&](const Names & keys) -> Names {
                fetched.emplace_back(keys.begin(), keys.end());
                Names answer;
                std::ranges::set_intersection(keys, fetchable, std::inserter(answer, answer.end()));
                valid.insert(answer.begin(), answer.end());
                return answer;
            },
            budget);
    }
};

void aFetchableInputIsNotBuilt()
{
    Fetching store{.valid = {}, .fetchable = {"x-out"}};
    assert((store.plan({{"t", {"t-out"}}}, 1) == Names{"t"}));
    assert((store.queried == Calls{{"t-out"}, {"x-out"}}));
    assert((store.fetched == Calls{{"t-out"}, {"x-out"}}));
}

// A lookup of the substituters lists an input signed by an untrusted key,
// but the server will not fetch it, so it is built.
void anInputTheStoreCannotFetchIsBuilt()
{
    Fetching store{.valid = {}, .fetchable = {}};
    assert((store.plan({{"t", {"t-out"}}}, 1) == Names{"t", "x", "w"}));
    assert((store.fetched == Calls{{"t-out"}, {"x-out"}, {"w-out"}}));
}

void aFetchableRootIsNotBuilt()
{
    Fetching store{.valid = {}, .fetchable = {"t-out", "x-out"}};
    assert(store.plan({{"t", {"t-out"}}}, 1).empty());
    assert((store.fetched == Calls{{"t-out"}}));
}

// A call also queries the levels below, but what they lack is fetched only
// once the walk reaches them.
void aHeldRootFetchesNothingBelowIt()
{
    Fetching store{.valid = {"t-out"}, .fetchable = {"x-out", "w-out"}};
    assert(store.plan({{"t", {"t-out"}}}, 64).empty());
    assert((store.queried == Calls{{"t-out", "w-out", "x-out"}}));
    assert(store.fetched.empty());
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
    aRootNeedsOnlyTheKeysAskedOfIt();
    aRebuiltRootIsMissingWhatTheStoreHolds();
    anInputNeedsOnlyTheKeysItsDependantsUse();
    anInputNeedsTheKeysOfAllItsDependants();
    anInputUsedOnlyByAHeldNodeIsNotNeeded();
    aRootAnotherBuildsOnNeedsWhatBothAsk();
    aFetchableInputIsNotBuilt();
    anInputTheStoreCannotFetchIsBuilt();
    aFetchableRootIsNotBuilt();
    aHeldRootFetchesNothingBelowIt();
    return 0;
} catch (const std::exception & err) {
    std::cerr << "top-down-test: " << err.what() << '\n';
    return 1;
}
