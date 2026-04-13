"""Sector network YAML data + loader.

Per sector, one YAML file (e.g. `defense.yaml`) holds the static actor graph
previously stored as a giant Python dict literal in
`intelligence/<sector>_network.py`. The single canonical entrypoint is
`intelligence.sector_networks.loader`.
"""

from .loader import (  # noqa: F401
    load_sector_network,
    get_sector_data,
    get_actors,
    list_sectors,
    SECTOR_MODULES,
)
