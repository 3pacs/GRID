# PRIVACY BOUNDARY: This module uses Hyperspace for local inference
# and embeddings only. No GRID signal logic, feature values, discovered
# cluster structures, or hypothesis details are sent to the network.
"""
GRID Hyperspace node monitoring module.

Tracks node health, points, peer count, and API status.  Provides a
terminal dashboard for quick operational checks.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path
from typing import Any

from loguru import logger as log

from hyperspace.client import HyperspaceClient

# Log file search paths (tried in order)
_LOG_PATHS: list[str] = [
    "/var/log/hyperspace-grid.log",
    "/tmp/hyperspace-grid.log",
    str(Path.home() / ".hyperspace" / "node.log"),
]


class HyperspaceMonitor:
    """Monitors the local Hyperspace node for GRID operational awareness.

    Provides structured status information, log tailing, and a
    terminal dashboard.

    Attributes:
        client: HyperspaceClient instance for API checks.
    """

    def __init__(self, hyperspace_client: HyperspaceClient) -> None:
        """Initialise the monitor.

        Parameters:
            hyperspace_client: A connected HyperspaceClient.
        """
        self.client = hyperspace_client
        log.info("HyperspaceMonitor initialised")

    def _run_cmd(self, args: list[str], timeout: int = 10) -> str | None:
        """Run a shell command and return stdout, or None on failure.

        Parameters:
            args: Command and arguments.
            timeout: Timeout in seconds.

        Returns:
            str: Standard output, or None on error.
        """
        try:
            result = subprocess.run(
                args,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            if result.returncode == 0:
                return result.stdout.strip()
            log.debug(
                "Command {cmd} returned {rc}: {err}",
                cmd=args[0],
                rc=result.returncode,
                err=result.stderr.strip()[:200],
            )
            return None
        except FileNotFoundError:
            log.debug("Command not found: {cmd}", cmd=args[0])
            return None
        except subprocess.TimeoutExpired:
            log.debug("Command timed out: {cmd}", cmd=args[0])
            return None
        except Exception as exc:
            log.debug("Command error: {err}", err=str(exc))
            return None

    def get_node_status(self) -> dict[str, Any]:
        """Get Hyperspace node identity and status via ``hyperspace hive whoami``.

        Returns:
            dict: Structured status with keys ``peer_id``, ``points_balance``,
                  ``uptime``, ``connected_peers``, or error information.
        """
        output = self._run_cmd(["hyperspace", "hive", "whoami"])
        if output is None:
            return {"error": "hyperspace CLI not available or node offline"}

        result: dict[str, Any] = {"raw_output": output}

        # Parse common fields from whoami output
        for line in output.split("\n"):
            line_lower = line.lower().strip()
            if "peer" in line_lower and "id" in line_lower:
                parts = line.split(":")
                if len(parts) >= 2:
                    result["peer_id"] = parts[-1].strip()
            elif "point" in line_lower:
                numbers = re.findall(r"[\d,]+\.?\d*", line)
                if numbers:
                    result["points_balance"] = float(numbers[0].replace(",", ""))
            elif "uptime" in line_lower:
                result["uptime"] = line.split(":", 1)[-1].strip() if ":" in line else line.strip()
            elif "peer" in line_lower and "connect" in line_lower:
                numbers = re.findall(r"\d+", line)
                if numbers:
                    result["connected_peers"] = int(numbers[0])

        return result

    def get_system_info(self) -> dict[str, Any]:
        """Get hardware information via ``hyperspace system-info``.

        Returns:
            dict: GPU model, VRAM, CPU info, or error dict.
        """
        output = self._run_cmd(["hyperspace", "system-info"])
        if output is None:
            return {"error": "hyperspace CLI not available"}

        result: dict[str, Any] = {"raw_output": output}

        for line in output.split("\n"):
            line_lower = line.lower().strip()
            if "gpu" in line_lower:
                result["gpu"] = line.split(":", 1)[-1].strip() if ":" in line else line.strip()
            elif "vram" in line_lower:
                result["vram"] = line.split(":", 1)[-1].strip() if ":" in line else line.strip()
            elif "cpu" in line_lower:
                result["cpu"] = line.split(":", 1)[-1].strip() if ":" in line else line.strip()
            elif "ram" in line_lower or "memory" in line_lower:
                result["ram"] = line.split(":", 1)[-1].strip() if ":" in line else line.strip()

        return result

    def get_points_summary(self) -> dict[str, Any]:
        """Get a summary of the node's points and connectivity.

        Returns:
            dict: Keys ``peer_id``, ``total_points``, ``session_uptime_hours``,
                  ``connected_peers``, ``api_available``, ``models_loaded``.
        """
        node_status = self.get_node_status()
        health = self.client.health_check()

        summary: dict[str, Any] = {
            "peer_id": node_status.get("peer_id", "unknown"),
            "total_points": node_status.get("points_balance", 0.0),
            "session_uptime_hours": 0.0,
            "connected_peers": node_status.get("connected_peers", 0),
            "api_available": health["available"],
            "models_loaded": health["models"],
        }

        # Parse uptime into hours
        uptime_str = node_status.get("uptime", "")
        if uptime_str:
            hours = 0.0
            h_match = re.search(r"(\d+)\s*h", uptime_str)
            m_match = re.search(r"(\d+)\s*m", uptime_str)
            if h_match:
                hours += float(h_match.group(1))
            if m_match:
                hours += float(m_match.group(1)) / 60
            summary["session_uptime_hours"] = round(hours, 2)

        return summary

    def tail_log(self, n_lines: int = 50) -> list[str]:
        """Read the last ``n_lines`` from the Hyperspace log file.

        Searches multiple known log paths in order.

        Parameters:
            n_lines: Number of trailing lines to return.

        Returns:
            list[str]: Log lines, or empty list if no log found.
        """
        for log_path in _LOG_PATHS:
            p = Path(log_path)
            if p.exists() and p.is_file():
                try:
                    lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
                    result = lines[-n_lines:] if len(lines) > n_lines else lines
                    log.debug("Tailed {n} lines from {p}", n=len(result), p=log_path)
                    return result
                except Exception as exc:
                    log.warning("Could not read log {p}: {e}", p=log_path, e=str(exc))

        log.debug("No Hyperspace log file found")
        return []

    def is_earning(self) -> bool:
        """Check whether the node is online and earning points.

        Returns True if the node has connected peers and the API is available.

        Returns:
            bool: True if the node appears to be earning.
        """
        try:
            summary = self.get_points_summary()
            return (
                summary["api_available"]
                and summary["connected_peers"] > 0
            )
        except Exception:
            return False


def print_status_dashboard() -> None:
    """Print a clean terminal status dashboard for the Hyperspace node."""
    from hyperspace.client import get_client

    client = get_client()
    monitor = HyperspaceMonitor(client)
    summary = monitor.get_points_summary()

    status = "ONLINE" if summary["api_available"] else "OFFLINE"
    peer_id = summary["peer_id"]
    if len(peer_id) > 12 and peer_id != "unknown":
        peer_id_display = peer_id[:12] + "..."
    else:
        peer_id_display = peer_id

    points = f"{summary['total_points']:,.0f}" if summary["total_points"] else "0"
    peers = summary["connected_peers"]

    hours = int(summary["session_uptime_hours"])
    mins = int((summary["session_uptime_hours"] - hours) * 60)
    uptime_str = f"{hours}h {mins:02d}m"

    api_check = "localhost:8080 OK" if summary["api_available"] else "localhost:8080 DOWN"
    model = summary["models_loaded"][0] if summary["models_loaded"] else "(none)"

    border_top = "\u2554" + "\u2550" * 34 + "\u2557"
    border_mid = "\u2560" + "\u2550" * 34 + "\u2563"
    border_bot = "\u255a" + "\u2550" * 34 + "\u255d"
    v = "\u2551"

    print(border_top)
    print(f"{v}  HYPERSPACE NODE  —  GRID        {v}")
    print(border_mid)
    print(f"{v}  Status:    {status:<22s}{v}")
    print(f"{v}  Peer ID:   {peer_id_display:<22s}{v}")
    print(f"{v}  Points:    {points:<22s}{v}")
    print(f"{v}  Peers:     {peers} connected{' ' * (13 - len(str(peers)))}{v}")
    print(f"{v}  Uptime:    {uptime_str:<22s}{v}")
    print(f"{v}  API:       {api_check:<22s}{v}")
    print(f"{v}  Model:     {model:<22s}{v}")
    print(border_bot)


if __name__ == "__main__":
    print_status_dashboard()
