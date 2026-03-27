"""LRU cache primitives backed by a hash map and doubly linked list."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class _Node:
    key: str
    prev: _Node | None = None
    next: _Node | None = None


class LRUTracker:
    def __init__(self) -> None:
        self._nodes: dict[str, _Node] = {}
        self._head = _Node("__head__")
        self._tail = _Node("__tail__")
        self._head.next = self._tail
        self._tail.prev = self._head

    def __contains__(self, key: str) -> bool:
        return key in self._nodes

    def __len__(self) -> int:
        return len(self._nodes)

    def touch(self, key: str) -> None:
        node = self._nodes.get(key)
        if node is None:
            node = _Node(key)
            self._nodes[key] = node
        else:
            self._detach(node)
        self._insert_after_head(node)

    def remove(self, key: str) -> None:
        node = self._nodes.pop(key, None)
        if node is None:
            return
        self._detach(node)

    def pop_lru(self) -> str | None:
        node = self._tail.prev
        if node is None or node is self._head:
            return None
        self.remove(node.key)
        return node.key

    def _detach(self, node: _Node) -> None:
        prev_node = node.prev
        next_node = node.next
        if prev_node is not None:
            prev_node.next = next_node
        if next_node is not None:
            next_node.prev = prev_node
        node.prev = None
        node.next = None

    def _insert_after_head(self, node: _Node) -> None:
        first = self._head.next
        node.prev = self._head
        node.next = first
        self._head.next = node
        if first is not None:
            first.prev = node

