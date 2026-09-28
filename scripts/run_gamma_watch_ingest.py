"""Disabled P1 design entrypoint. No IO, network, database or collector imports."""
import os


def main():
    flag = os.environ.get("GRID_ENABLE_GAMMA_WATCH_INGEST_JOB", "false").lower()
    if flag == "false":
        print("Gamma Watch ingestion disabled: P1 design only")
        return 0
    if flag != "true":
        raise SystemExit("GRID_ENABLE_GAMMA_WATCH_INGEST_JOB must be true or false; blank is invalid")
    raise SystemExit("Activation unavailable: writer, replay dry-run and owner GO require a separate reviewed PR")


if __name__ == "__main__":
    raise SystemExit(main())
