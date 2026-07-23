"""Credit & fixed-income (FRED/ALFRED vintages): OAS by rating, financial-
conditions & stress indices, curve/CP/mortgage spreads. Vintage-clean PIT."""
from __future__ import annotations

from worldstate.collectors.fred_base import FredVintageBase


class CreditFred(FredVintageBase):
    domain = "credit"
    source = "fred"
    SERIES = [
        # ICE BofA option-adjusted spreads
        "BAMLC0A0CM", "BAMLC0A1CAAA", "BAMLC0A2CAA", "BAMLC0A3CA", "BAMLC0A4CBBB",
        "BAMLH0A0HYM2", "BAMLH0A1HYBB", "BAMLH0A2HYB", "BAMLH0A3HYC",
        "BAMLEMCBPIOAS",
        # Moody's corporate yields & spreads
        "DAAA", "DBAA", "AAA10Y", "BAA10Y",
        # financial conditions / stress
        "NFCI", "ANFCI", "STLFSI4",
        # money-market & mortgage spreads
        "TEDRATE", "DPRIME", "DCPF3M", "DCPN3M",
        "MORTGAGE30US", "MORTGAGE15US",
        # bank lending standards (SLOOS)
        "DRTSCILM", "DRTSCIS",
    ]
