"""Commodities (FRED/ALFRED vintages): energy, metals, agriculture prices +
indices. Daily price series are effectively immutable; captured with vintages."""
from __future__ import annotations

from worldstate.collectors.fred_base import FredVintageBase


class CommoditiesFred(FredVintageBase):
    domain = "commodity"
    source = "fred"
    SERIES = [
        # energy
        "DCOILWTICO", "DCOILBRENTEU", "DHHNGSP", "DJFUELUSGULF", "GASREGCOVW",
        "PNGASEUUSDM", "PCOALAUUSDM",
        # metals -- GOLDAMGBD228NLBM/SLVPRUSD (ICE fixings) were pulled from FRED
        # in Jan 2022 and PPLAT no longer exists; all three 400 permanently.
        "PCOPPUSDM", "PALUMUSDM",
        # agriculture
        "PWHEAMTUSDM", "PMAIZMTUSDM", "PSOYBUSDQ", "PSUGAISAUSDM", "PCOTTINDUSDM",
        # aggregate indices
        "PALLFNFINDEXQ", "PPIACO",
    ]
