"""Real estate (FRED/ALFRED vintages): home prices, starts/permits/sales, supply,
vacancy, homeownership. Vintage-clean PIT."""
from __future__ import annotations

from worldstate.collectors.fred_base import FredVintageBase


class RealEstateFred(FredVintageBase):
    domain = "real_estate"
    source = "fred"
    SERIES = [
        # prices
        "CSUSHPINSA", "CSUSHPISA", "SPCS20RSA", "USSTHPI", "MSPUS", "ASPUS",
        # activity: starts / permits / sales / completions
        "HOUST", "HOUST1F", "PERMIT", "PERMIT1", "HSN1F", "EXHOSLUSM495S",
        "COMPUTSA", "HNFSEPUSSA",
        # supply / vacancy / ownership
        "MSACSR", "RRVRUSQ156N", "RHORUSQ156N", "RSAHORUSQ156S",
        # rates / affordability
        "MORTGAGE30US", "FIXHAI",
    ]
