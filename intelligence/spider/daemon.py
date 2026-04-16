"""Spider daemon — continuous connection mapping loop.

Runs as a systemd service. Pops actors from the priority queue,
expands their connections via the discovery orchestrator, persists
results to Postgres, and updates the in-memory graph.

Usage:
    python -m intelligence.spider.daemon
"""

from __future__ import annotations

import sys
import time
from typing import Any

from loguru import logger as log


def run_spider(
    max_rounds: int = 0,
    sleep_between: float = 2.0,
    idle_sleep: float = 60.0,
    load_actor_limit: int | None = None,
    load_connection_limit: int | None = None,
    max_new_connections: int = 50,
    seed_limit: int = 5000,
) -> None:
    """Main spider loop.

    Args:
        max_rounds: 0 = run forever. >0 = stop after N expansions.
        sleep_between: seconds to sleep between expansions.
        idle_sleep: seconds to sleep when the queue is empty in daemon mode.
        load_actor_limit: cap on actors loaded into memory.
        load_connection_limit: cap on actor_connections loaded into memory.
        max_new_connections: cap persisted/enqueued connections per expanded actor.
        seed_limit: maximum high-priority actors to keep in the in-memory queue.
    """
    sys.path.insert(0, ".")
    from db import get_engine

    from intelligence.actors.db import ensure_spider_tables, save_actor, save_connection
    from intelligence.spider.discovery import DiscoveryOrchestrator
    from intelligence.entity_resolver import SpiderEntityResolver as EntityResolver
    from intelligence.spider.graph_engine import GraphEngine
    from intelligence.spider.priority_queue import PriorityQueue
    from intelligence.spider.sources.google_kg import GoogleKgAdapter
    from intelligence.spider.sources.icij_offshore import IcijOffshoreAdapter
    from intelligence.spider.sources.news_cooccurrence import NewsCooccurrenceAdapter
    from intelligence.spider.sources.opencorporates import OpenCorporatesAdapter
    from intelligence.spider.sources.operator_input import OperatorInputAdapter
    from intelligence.spider.sources.sec_crossref import SecCrossRefAdapter
    from intelligence.spider.sources.wikidata import WikidataAdapter

    engine = get_engine()
    ensure_spider_tables(engine)

    graph = GraphEngine()
    log.info("Loading actor graph from database...")
    graph.load_from_db(
        engine,
        actor_limit=load_actor_limit,
        connection_limit=load_connection_limit,
    )
    log.info("Graph loaded: {a} actors, {c} connections", a=graph.actor_count, c=graph.connection_count)

    resolver = EntityResolver(graph)
    adapters = [
        WikidataAdapter(),
        SecCrossRefAdapter(),
        IcijOffshoreAdapter(),
        OpenCorporatesAdapter(),
        NewsCooccurrenceAdapter(),
        GoogleKgAdapter(),
        OperatorInputAdapter(),
    ]
    orchestrator = DiscoveryOrchestrator(graph=graph, resolver=resolver, adapters=adapters)
    queue = PriorityQueue()

    _seed_queue(graph, queue, limit=seed_limit)
    log.info("Spider queue seeded: {d} actors pending", d=queue.depth)

    rounds = 0
    while True:
        actor_id = queue.pop()
        if actor_id is None:
            if max_rounds > 0:
                log.info("Queue empty — bounded spider run complete after {r} rounds", r=rounds)
                break
            log.info("Queue empty — spider sleeping {s}s before re-seeding", s=idle_sleep)
            time.sleep(idle_sleep)
            _seed_queue(graph, queue, limit=seed_limit)
            continue

        log.info("Expanding: {a} (queue={q}, done={d})", a=actor_id, q=queue.depth, d=queue.total_done)

        try:
            new_actors, new_connections = orchestrator.expand(actor_id)
            if max_new_connections > 0 and len(new_connections) > max_new_connections:
                new_connections = sorted(
                    new_connections,
                    key=lambda item: item[2].strength,
                    reverse=True,
                )[:max_new_connections]
                kept_ids = {target_id for _, target_id, _ in new_connections}
                new_actors = [actor for actor in new_actors if actor["id"] in kept_ids]

            for actor_data in new_actors:
                save_actor(engine, actor_data["id"], actor_data)

            for source_id, target_id, meta in new_connections:
                save_connection(engine, source_id, target_id, meta)

            for actor_data in new_actors:
                degree = actor_data.get("degree", 0)
                if degree <= 11:
                    influence = actor_data.get("influence_score", 0.3)
                    queue.push(actor_data["id"], priority=queue.compute_score(
                        influence=influence,
                        evidence_density=len(actor_data.get("data_sources", [])) / 10.0,
                        frontier_ratio=1.0,
                    ))

            queue.mark_done(actor_id, connections_found=len(new_connections), actors_created=len(new_actors))

        except Exception as exc:
            log.error("Spider expansion failed for {a}: {e}", a=actor_id, e=str(exc))
            queue.mark_done(actor_id)

        rounds += 1
        if max_rounds > 0 and rounds >= max_rounds:
            log.info("Spider completed {r} rounds, stopping", r=rounds)
            break

        time.sleep(sleep_between)


def _seed_queue(graph: Any, queue: Any, limit: int = 5000) -> None:
    """Seed the queue with all actors that haven't been fully explored."""
    items = sorted(
        graph._actors.items(),
        key=lambda item: item[1].get("influence_score", 0.3),
        reverse=True,
    )
    if limit > 0:
        items = items[:limit]

    for actor_id, data in items:
        if actor_id not in queue._done:
            influence = data.get("influence_score", 0.3)
            evidence = len(data.get("data_sources", []))
            neighbors = len(graph.get_neighbors(actor_id))
            frontier = 1.0 - (neighbors / max(neighbors, 10))
            queue.push(actor_id, priority=queue.compute_score(
                influence=influence,
                evidence_density=min(evidence / 10.0, 1.0),
                frontier_ratio=frontier,
            ))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run bounded GRID actor graph spider expansions.")
    parser.add_argument("--once", action="store_true", help="Run a bounded batch and exit.")
    parser.add_argument("--max-rounds", type=int, default=0, help="0 runs forever unless --once is set.")
    parser.add_argument("--sleep-between", type=float, default=2.0, help="Seconds between actor expansions.")
    parser.add_argument("--idle-sleep", type=float, default=60.0, help="Seconds to sleep after an empty queue.")
    parser.add_argument("--load-actor-limit", type=int, default=0, help="Cap DB actors loaded; 0 loads all.")
    parser.add_argument("--load-connection-limit", type=int, default=0, help="Cap DB edges loaded; 0 loads all.")
    parser.add_argument("--max-new-connections", type=int, default=50, help="Cap persisted edges per actor; 0 disables.")
    parser.add_argument("--seed-limit", type=int, default=5000, help="Maximum actors placed in memory queue.")
    args = parser.parse_args()

    max_rounds = args.max_rounds
    if args.once and max_rounds <= 0:
        max_rounds = 25

    log.remove()
    log.add(sys.stderr, level="INFO")
    run_spider(
        max_rounds=max_rounds,
        sleep_between=args.sleep_between,
        idle_sleep=args.idle_sleep,
        load_actor_limit=args.load_actor_limit or None,
        load_connection_limit=args.load_connection_limit or None,
        max_new_connections=args.max_new_connections,
        seed_limit=args.seed_limit,
    )
