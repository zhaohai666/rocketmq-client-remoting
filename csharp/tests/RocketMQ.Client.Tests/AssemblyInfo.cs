// Test project global: **test parallelisation is turned off**.
// LoggingTests temporarily changes the global log file/level and asserts on the number of
// lines written; TopAddressingTests sets the global environment variable
// ROCKETMQ_NAMESRV_DOMAIN -- both are non-thread-safe process-level state, and once they
// run in parallel with other test classes they pollute each other (observed as INFO lines
// from concurrent threads bleeding into LevelFilter_SuppressesDebugAtInfo). The full suite
// is seconds anyway, so running it serially costs no throughput.
using Xunit;

[assembly: CollectionBehavior(DisableTestParallelization = true)]
