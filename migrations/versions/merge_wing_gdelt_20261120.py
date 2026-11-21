"""Join the wing-IV proxy and GDELT GKG migration heads.

Revision ID: merge_wing_gdelt_20261120
Revises: wing_iv_proxy_20261116, gdelt_gkg_20261116

Both chains branch from options_contract_clocks_20261114:

  wing_iv_proxy_20261116 <- options_contract_clocks_20261114
  gdelt_gkg_20261116 <- gdelt_bulk_loader_20261116 <- options_contract_clocks_20261114

Pure merge point: no schema or data operations. Downgrade refuses, because
wing_iv_proxy_20261116 refuses to erase wing IV lineage.
"""

revision = "merge_wing_gdelt_20261120"
down_revision = (
    "wing_iv_proxy_20261116",
    "gdelt_gkg_20261116",
)
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    raise RuntimeError(
        "Downgrade through merge_wing_gdelt_20261120 is refused: "
        "wing_iv_proxy_20261116 does not erase wing IV lineage."
    )
