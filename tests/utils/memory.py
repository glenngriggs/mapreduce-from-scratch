"""A class for profiling memory usage during unit tests."""
import tracemalloc
import time


class MemoryProfiler:
    """Monitor memory usage."""

    def __init__(self):
        """Initialize member variables."""
        self.mem_before = None
        self.mem_max = None
        self.time_start = None
        self.time_stop = None

    def start(self):
        """Start profiler."""
        self.time_start = time.time()
        tracemalloc.start()
        self.mem_before, _ = tracemalloc.get_traced_memory()
        tracemalloc.reset_peak()

    def stop(self):
        """Stop profiler."""
        _, self.mem_max = tracemalloc.get_traced_memory()
        self.time_stop = time.time()
        tracemalloc.stop()

    def __enter__(self):
        """Make this class a context manager."""
        return self

    def __exit__(self, *args, **kwargs):
        """Stop tracing memory allocations if the context exits.

        We need this in case an exception prevents MemoryProfiler.stop() from
        being called. If it is called, this is a no-op.
        """
        tracemalloc.stop()

    def get_mem_delta(self):
        """Return max difference in memory usage (B) since start."""
        return self.mem_max - self.mem_before

    def get_time_delta(self):
        """Return time difference in seconds from start to stop."""
        return self.time_stop - self.time_start
