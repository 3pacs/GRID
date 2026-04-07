"""Tests for the spider priority queue."""

from intelligence.spider.priority_queue import PriorityQueue


def test_score_actor():
    pq = PriorityQueue()
    score = pq.compute_score(
        influence=0.9,
        evidence_density=0.6,
        frontier_ratio=0.3,
    )
    expected = 0.9 * 0.4 + 0.6 * 0.3 + 0.3 * 0.3
    assert abs(score - expected) < 0.001


def test_push_and_pop():
    pq = PriorityQueue()
    pq.push("actor_a", priority=0.8)
    pq.push("actor_b", priority=0.95)
    pq.push("actor_c", priority=0.5)

    assert pq.pop() == "actor_b"
    assert pq.pop() == "actor_a"
    assert pq.pop() == "actor_c"


def test_pop_empty_returns_none():
    pq = PriorityQueue()
    assert pq.pop() is None


def test_depth():
    pq = PriorityQueue()
    pq.push("a", priority=0.5)
    pq.push("b", priority=0.9)
    assert pq.depth == 2
    pq.pop()
    assert pq.depth == 1


def test_mark_done():
    pq = PriorityQueue()
    pq.push("a", priority=0.5)
    pq.mark_done("a", connections_found=5, actors_created=2)
    assert pq.depth == 0
    assert pq.pop() is None
