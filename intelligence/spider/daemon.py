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


def run_spider(max_rounds: int = 0, sleep_between: float = 2.0) -> None:
    """Main spider loop.

    Args:
        max_rounds: 0 = run forever. >0 = stop after N expansions.
        sleep_between: seconds to sleep between expansions.
    """
    sys.path.insert(0, ".")
    from db import get_engine

    from intelligence.spider.db import ensure_spider_tables, save_actor, save_connection
    from intelligence.spider.discovery import DiscoveryOrchestrator
    from intelligence.spider.entity_resolver import EntityResolver
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
    graph.load_from_db(engine)
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

    _seed_queue(graph, queue)
    log.info("Spider queue seeded: {d} actors pending", d=queue.depth)

    rounds = 0
    while True:
        actor_id = queue.pop()
        if actor_id is None:
            log.info("Queue empty — spider sleeping 60s before re-seeding")
            time.sleep(60)
            _seed_queue(graph, queue)
            continue

        log.info("Expanding: {a} (queue={q}, done={d})", a=actor_id, q=queue.depth, d=queue.total_done)

        try:
            new_actors, new_connections = orchestrator.expand(actor_id)

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


def _seed_queue(graph: Any, queue: Any) -> None:
    """Seed the queue with all actors that haven't been fully explored."""
    for actor_id, data in graph._actors.items():
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
    log.remove()
    log.add(sys.stderr, level="INFO")
    run_spider()
