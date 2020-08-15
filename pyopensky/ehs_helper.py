import pandas as pd
import pyModeS as pms
from typing import Iterable
from pyopensky.impala_wrapper import OpenskyImpalaWrapper

SUPPORT_BDS = ["BDS40", "BDS50", "BDS60"]


class EHSHelper(object):
    """docstring for EHSHelper."""

    def __init__(self):
        super(EHSHelper, self).__init__()
        self.opensky = OpenskyImpalaWrapper()
        self.bds_codes = SUPPORT_BDS

    def require_bds(self, bds):
        """Set required BDS code.

        Args:
            bds (str or list): Desired BDS code, must be a subset of ["BDS40", "BDS50", "BDS60"]

        """
        if isinstance(bds, str):
            self.bds_codes = [bds.upper()]
        elif isinstance(bds, Iterable):
            self.bds_codes = [x.upper() for x in bds]

        for x in self.bds_codes:
            if x not in SUPPORT_BDS:
                raise RuntimeError(
                    "BDS codes must be a subset of (%s)." % ",".join(SUPPORT_BDS)
                )

    def get(self, icao24, start, end, bound=None):
        """Get decoded EHS data.

        Args:
            icao24 (str or list): Filter of one or a list of IACO addresses, default to None
            start (str): Start of time period with format YYYY-MM-DD HH:MM:SS
            end (str): End of time period with format YYYY-MM-DD HH:MM:SS
            bound (list): Filter latitude and longitude bound with format of [lat1, lon1, lat2, lon2], default to None
        Returns:
            pandas.DataFrame: Impala query results

        """
        df = self.opensky.query(
            type="raw", start=start, end=end, icao24=icao24, bound=bound
        )

        if df is None:
            return

        df = df.drop_duplicates(subset=["icao24", "rawmsg"])

        print("* Processing data...")

        df = df.sort_values("mintime")
        df["DF"] = df["rawmsg"].apply(pms.df)

        commb = df[df["DF"].isin([20, 21])][["icao24", "mintime", "rawmsg", "DF"]]

        commb.loc[commb.DF == 20, "altitude"] = commb.loc[
            commb.DF == 20, "rawmsg"
        ].apply(pms.altcode)

        commb.loc[commb.DF == 21, "squawk"] = commb.loc[commb.DF == 21, "rawmsg"].apply(
            pms.idcode
        )

        if "BDS44" in self.bds_codes or "BDS45" in self.bds_codes:
            include_mrar = True
        else:
            include_mrar = False

        commb["bds"] = commb["rawmsg"].apply(pms.bds.infer, args=[include_mrar])

        ehs = commb[commb["bds"].isin(self.bds_codes)]

        # construct colums of the data frame based on required BDS codes
        columns = ["icao24", "time", "rawmsg", "bds", "altitude", "squawk"]
        if "BDS40" in self.bds_codes:
            columns.extend(["selalt40mcp", "selalt40fms", "p40baro"])
        if "BDS50" in self.bds_codes:
            columns.extend(["roll50", "trk50", "rtrk50", "gs50", "tas50"])
        if "BDS60" in self.bds_codes:
            columns.extend(["ias60", "hdg60", "mach60", "vr60baro", "vr60ins"])
        dfout = pd.DataFrame(columns=columns)

        # decode messages row by row
        for i, r in ehs.iterrows():
            bds = r["bds"]
            msg = r["rawmsg"]

            d = {
                "time": r["mintime"],
                "icao24": r["icao24"],
                "bds": bds,
                "rawmsg": r["rawmsg"],
                "altitude": r["altitude"],
                "squawk": r["squawk"],
            }

            if bds == "BDS40":
                selalt40mcp = pms.commb.selalt40mcp(msg)
                selalt40fms = pms.commb.selalt40fms(msg)
                p40baro = pms.commb.p40baro(msg)

                d.update(
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

                d.update(
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

                d.update(
                    {
                        "ias60": ias60,
                        "hdg60": hdg60,
                        "mach60": mach60,
                        "vr60baro": vr60baro,
                        "vr60ins": vr60ins,
                    }
                )

            dfout = dfout.append(d, ignore_index=True)
        return dfout
