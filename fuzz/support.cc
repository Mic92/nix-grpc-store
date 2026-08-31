// Linked into every fuzz target.

// libnixstore's readString() allocates whatever 64-bit length the peer sends,
// so huge allocations are expected; let them fail as std::bad_alloc (which
// production code catches) instead of an ASan report. scripts/fuzz.sh raises
// libFuzzer's -malloc_limit_mb to match. DummyStore and libstore globals are
// never freed, hence no leak check.
// NOLINTNEXTLINE(bugprone-reserved-identifier,readability-identifier-naming)
extern "C" auto __asan_default_options() -> const char *
{
    return "allocator_may_return_null=1:detect_leaks=0";
}
