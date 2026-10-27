"""Proposed API-only rollback interface; never invokes legacy shared rollback."""
import argparse


def rollback(binding, approval, kernel, service):
    """Use the same fenced transaction implementation; no legacy helper call.

Only explicitly injected capabilities can perform operations. The CLI does not
construct capabilities and retains unconditional refusal before input reads.
"""
    from api_isolation_adapter import Adapter
    from deploy_api_isolated import recover
    return recover(binding, Adapter(kernel, service), approval)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binding", required=True)
    parser.add_argument("--approval", required=True)
    parser.add_argument("--execute", action="store_true", required=True)
    parser.parse_args()
    parser.exit(78, "NOT_ADOPTED: API rollback native adapter absent; no pointer, service, cleanup or DB action executed.\n")


if __name__ == "__main__":
    main()
