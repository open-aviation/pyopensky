from __future__ import annotations

from typing import Any

import pyModeS as pms

import pandas as pd


def decode_row(row: pd.Series) -> dict[str, Any]:
    bds = row["bds"]
    msg = row["rawmsg"]

    decoded = {
        "time": row["mintime"],
        "icao24": row["icao24"],
        "bds": bds,
        "rawmsg": row["rawmsg"],
        "altitude": row["altitude"],
        "squawk": row["squawk"],
    }

    if bds == "BDS40":
        selalt40mcp = pms.commb.selalt40mcp(msg)
        selalt40fms = pms.commb.selalt40fms(msg)
        p40baro = pms.commb.p40baro(msg)

        decoded.update(
            {
                "selalt40mcp": selalt40mcp,
                "selalt40fms": selalt40fms,
                "p40baro": p40baro,
            }
        )

    if bds == "BDS50":
        roll50 = pms.commb.roll50(msg)
        trk50 = pms.commb.trk50(msg)
        rtrk50 = pms.commb.rtrk50(msg)
        gs50 = pms.commb.gs50(msg)
        tas50 = pms.commb.tas50(msg)

        decoded.update(
            {
                "roll50": roll50,
                "trk50": trk50,
                "rtrk50": rtrk50,
                "gs50": gs50,
                "tas50": tas50,
            }
        )

    if bds == "BDS60":
        ias60 = pms.commb.ias60(msg)
        hdg60 = pms.commb.hdg60(msg)
        mach60 = pms.commb.mach60(msg)
        vr60baro = pms.commb.vr60baro(msg)
        vr60ins = pms.commb.vr60ins(msg)

        decoded.update(
            {
                "ias60": ias60,
                "hdg60": hdg60,
                "mach60": mach60,
                "vr60baro": vr60baro,
                "vr60ins": vr60ins,
            }
        )
    return decoded


def decode(df: pd.DataFrame) -> pd.DataFrame:
    commb = (
        df.assign(DF=lambda df: df["rawmsg"].apply(pms.df))
        .query("DF in [20, 21]")
        .assign(bds=lambda df: df["rawmsg"].apply(pms.bds.infer))
        .query('bds in ["BDS40", "BDS50", "BDS60"]')
        .drop_duplicates(["icao24", "mintime"])
    )

    altcode = commb.loc[commb.DF == 20, "rawmsg"].apply(pms.altcode)
    commb.loc[commb.DF == 20, "altitude"] = altcode

    idcode = commb.loc[commb.DF == 21, "rawmsg"].apply(pms.idcode)
    commb.loc[commb.DF == 21, "squawk"] = idcode

    return pd.DataFrame.from_records(
        decode_row(row) for _i, row in commb.sort_values("mintime").iterrows()
    )
