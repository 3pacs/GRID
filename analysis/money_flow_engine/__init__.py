"""GRID — Money Flow Engine.

8-layer junction point model for tracking ~$500T in global capital flows.

Public API:
    build_flow_map(engine, as_of=None) -> FlowMap

Layers (in Sankey order):
    0. Monetary   — CB balance sheets, repo, TGA, global M2
    1. Credit     — Bank lending, bond spreads, money markets
    2. Institutional — 13F filings, pension flows, SWF
    3. Market     — ETF flows, options, dark pools
    4. Corporate  — Buybacks, dividends, M&A, IPOs
    5. Sovereign  — FX reserves, trade balance, tariffs
    6. Retail     — Margin debt, sentiment, fund flows
    7. Crypto     — BTC, stablecoins, on-chain flows
"""
from __future__ import annotations

from datetime import date, datetime, timezone

from loguru import logger as log
from sqlalchemy.engine import Engine

from .flow_inference import infer_flow_edges
from .layer_corporate import build_corporate_layer
from .layer_credit import build_credit_layer
from .layer_crypto import build_crypto_layer
from .layer_institutional import build_institutional_layer
from .layer_market import build_market_layer
from .layer_monetary import build_monetary_layer
from .layer_retail import build_retail_layer
from .layer_sovereign import build_sovereign_layer
from .types import FlowLayer, FlowMap

_LAYER_BUILDERS = (
    ("monetary", build_monetary_layer),
    ("credit", build_credit_layer),
    ("institutional", build_institutional_layer),
    ("market", build_market_layer),
    ("corporate", build_corporate_layer),
    ("sovereign", build_sovereign_layer),
    ("retail", build_retail_layer),
    ("crypto", build_crypto_layer),
)


def build_flow_map(engine: Engine, as_of: date | None = None) -> FlowMap:
    """Build the complete 8-layer flow map.

    Each layer is built independently (can be parallelized later).
    Flow edges are inferred from structural channel definitions.

    Returns an immutable FlowMap with all layers and edges.
    """
    if as_of is None:
        as_of = date.today()

    log.info("Building 8-layer flow map for {d}", d=as_of)

    layers_list: list[FlowLayer] = []
    for name, builder in _LAYER_BUILDERS:
        try:
            layer = builder(engine, as_of)
            layers_list.append(layer)
            log.info("Layer {n}: {c} nodes, confidence={conf}",
                     n=name, c=len(layer.nodes), conf=layer.confidence)
        except Exception as exc:
            log.error("Layer {n} failed: {e}", n=name, e=str(exc))
            layers_list.append(FlowLayer(
                id=name, label=name.title(), order=len(layers_list),
                nodes=(), confidence="estimated",
            ))

    layers = tuple(layers_list)

    # Infer flow edges between layers
    try:
        edges = infer_flow_edges(layers)
        log.info("Inferred {n} flow edges", n=len(edges))
    except Exception as exc:
        log.error("Flow inference failed: {e}", e=str(exc))
        edges = ()

    # Compute global aggregates from monetary layer
    monetary = next((la for la in layers if la.id == "monetary"), None)
    global_liquidity = monetary.total_value_usd if monetary else None
    global_liquidity_change = monetary.net_flow_1m if monetary else None

    # Policy score: stress_score is 0-1, map to -1..+1 where 0.5 = neutral
    policy_score = None
    if monetary and monetary.stress_score is not None:
        policy_score = round(1.0 - 2.0 * monetary.stress_score, 2)

    flow_map = FlowMap(
        layers=layers,
        edges=edges,
        global_liquidity_total=global_liquidity,
        global_liquidity_change_1m=global_liquidity_change,
        global_policy_score=policy_score,
        generated_at=datetime.now(timezone.utc).isoformat(),
        narrative="",  # filled by narrative module later
    )

    log.info("Flow map complete: {nl} layers, {ne} edges, liquidity={liq}",
             nl=len(layers), ne=len(edges),
             liq=f"${global_liquidity / 1e12:.1f}T" if global_liquidity else "N/A")

    return flow_map
