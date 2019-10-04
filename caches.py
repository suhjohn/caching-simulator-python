from abc import ABC, abstractmethod
from collections import deque
from typing import TypeVar, Dict

__all__ = [
    "Cache",
    "FIFOCache",
    "LRUCache"
]


class SimulatedCache(ABC):
    def __init__(self, capacity):
        self.capacity = capacity

    @abstractmethod
    def put(self, key: int, size: int):
        pass

    @abstractmethod
    def get(self, key: int):
        pass


class FIFOCache(SimulatedCache):
    def __init__(self, capacity):
        super().__init__(capacity)
        self.curr_capacity = 0
        self.q = deque()
        self.items = dict()

    def __str__(self):
        return f"{FIFOCache}: {self.curr_capacity}/{self.capacity}"

    def put(self, key: int, size: int):
        while self.capacity < self.curr_capacity + size:
            popped_key, popped_size = self.q.pop()
            self.curr_capacity -= popped_size
            del self.items[popped_key]

        self.q.appendleft((key, size))
        self.items[key] = size
        self.curr_capacity += size

    def get(self, key: int):
        return self.items.get(key)


class LRUNode:
    def __init__(self, key, size, prev, next):
        self.key = key
        self.size = size
        self.prev = prev
        self.next = next


class LRUCache(SimulatedCache):
    def __init__(self, capacity):
        """
        [] capacity 5
        {} forward
        {} backward

        1 <-> 2 <-> 3 <-> 4 <->
        {

        }

        :param capacity:
        """
        super().__init__(capacity)
        self.curr_capacity = 0
        self.map: Dict[int, LRUNode] = {}
        self.least_recent = None
        self.most_recent = None

    def __str__(self):
        return f"{LRUCache}: {self.curr_capacity}/{self.capacity}"

    def put(self, key: int, size: int):
        assert size < self.capacity

        while self.capacity < self.curr_capacity + size:
            to_evict = self.least_recent
            self.curr_capacity -= to_evict.size
            self.least_recent.next.prev = None
            self.least_recent = self.least_recent.next
            del self.map[to_evict.key]

        node: LRUNode = self.map.get(key)
        # reset pointers for existing node
        if node is not None:
            self._reset_links(node)
            self.curr_capacity -= node.size
            node.size = size
        else:
            node = LRUNode(key, size, None, None)
            self.map[node.key] = node

        self._add_to_head(node)
        self.curr_capacity += node.size

    def _add_to_head(self, node):
        if self.least_recent is None:
            self.least_recent = node
            self.most_recent = node
        else:
            self.most_recent.next = node
            node.prev = self.most_recent
            self.most_recent = node

    def _reset_links(self, node):
        if node.next:
            if node.prev:
                node.next.prev = node.prev
            else:
                node.next.prev = None
        if node.prev:
            if node.next:
                node.prev.next = node.next
            else:
                node.prev.next = None

    def get(self, key: int):
        node = self.map.get(key)
        if not node:
            return None
        self._reset_links(node)
        self._add_to_head(node)
        return node.size


class RandomCache(SimulatedCache):
    def __init__(self, capacity):
        super().__init__(capacity)
        self.curr_capacity = 0

    def __str__(self):
        return f"{RandomCache}: {self.curr_capacity}/{self.capacity}"

    def put(self, key: int, size: int):
        pass

    def get(self, key: int):
        pass


Cache = TypeVar("Cache", FIFOCache, LRUCache, RandomCache)
