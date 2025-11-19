"""Thread-safe OrderedDict."""
from typing import Any, TypeVar
import threading
from collections import OrderedDict

K = TypeVar("K")  # Key type for generic class
V = TypeVar("V")  # Value type for generic class


class ThreadSafeOrderedDict(OrderedDict[K, V]):
    """Thread-safe OrderdedDict."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Store a lock in the new OrderedDict."""
        self.lock = threading.Lock()
        super().__init__(*args, **kwargs)

    def __setitem__(self, key: K, value: V) -> None:
        """Acquire the lock before setting a new value."""
        with self.lock:
            super().__setitem__(key, value)

    def __getitem__(self, key: K) -> V:
        """Acquire the lock before retrieving a value."""
        with self.lock:
            return super().__getitem__(key)

    def __contains__(self, key: object) -> bool:
        """Acquire the lock before checking for key membership."""
        with self.lock:
            return super().__contains__(key)

    def values(self) -> list[V]:    # type: ignore[override]
        """Acquire the lock before retrieving all the values."""
        # Return a list instead of a `ValuesView` to avoid
        # dynamically updating values, which could cause a
        # race condition.
        with self.lock:
            return list(super().values())

    def items(self) -> list[tuple[K, V]]:   # type: ignore[override]
        """Acquire the lock before retrieving all the items."""
        # Return a list instead of an `ItemsView` to avoid
        # dynamically updating items, which could cause a
        # race condition.
        with self.lock:
            return list(super().items())
