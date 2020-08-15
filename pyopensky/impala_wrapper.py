import os
import re
import time
import pandas as pd
import configparser
import logging
import warnings
from io import StringIO
from typing import Iterable
from pathlib import Path
from pyopensky.ssh_client import SSHClient

# logging.basicConfig(level=logging.DEBUG)
# logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)

warnings.filterwarnings(action="ignore", module=".*paramiko.*")

# check login config file
homedir = str(Path.home())
config_path = homedir + "/.config/pyopensky/secret.conf"

if not os.path.exists(config_path):
    os.makedirs(os.path.dirname(config_path), exist_ok=True)
    with open(config_path, "w") as f:
        f.write("[default]\n")
        f.write("server = data.opensky-network.org\n")
        f.write("port = 2230\n")
        f.write("username = \n")
        f.write("password = \n")

config = configparser.ConfigParser()
config.read(homedir + "/.config/pyopensky/secret.conf")

SERVER = config.get("default", "server")
PORT = config.get("default", "port")
USERNAME = config.get("default", "username")
PASSWORD = config.get("default", "password")

if not USERNAME or not PASSWORD:
    raise RuntimeError(
        "Opensky Impala username and password are empty in {}".format(config_path)
    )


class OpenskyImpalaWrapper(SSHClient):
    """docstring for OpenskyImpalaWrapper."""

    def __init__(self):
        SSHClient.__init__(self)
        self.connect_opensky()

    def connect_opensky(self):
        self.connect(
            SERVER,
            port=PORT,
            username=USERNAME,
            password=PASSWORD,
            look_for_keys=False,
            allow_agent=False,
            compress=True,
        )

    def disconnect_opensky(self):
        self.disconnect()

    def check_and_reconnect(self):
        try:
            transport = self.get_transport()
            transport.send_ignore()
        except EOFError:
            print("Connection lost, reconnecting...")
            self.connect_opensky()

    def query(self, type, start, end, **kwargs):
        """Query opensky impala database.

        Args:
            type (str): Type of messages "adsb" or "raw"
            start (str): Start of time period with format YYYY-MM-DD HH:MM:SS
            end (str): End of time period with format YYYY-MM-DD HH:MM:SS
            icao24 (str or list): Filter of one or a list of IACO addresses, default to None
            bound (list): Filter latitude and longitude bound with format of [lat1, lon1, lat2, lon2], default to None
            countfirst (bool): Count and print number of records before actual query, default to True
            limit (int): Return only a number of return records, defualt to None
        Returns:
            pandas.DataFrame: Impala query results

        """
        icao24 = kwargs.get("icao24", None)
        bound = kwargs.get("bound", None)
        countfirst = kwargs.get("countfirst", True)
        limit = kwargs.get("limit", None)

        ts_start = pd.Timestamp(start, tz="utc").timestamp()
        ts_end = pd.Timestamp(end, tz="utc").timestamp()

        hour_start = ts_start // 3600 * 3600
        hour_end = (ts_end // 3600 + 1) * 3600

        if type == "adsb":
            table = "state_vectors_data4"
            time_col = "time"
        elif type == "raw":
            table = "rollcall_replies_data4"
            time_col = "mintime"
        else:
            raise RuntimeError("Unknown query type: {}".format(type))

        if isinstance(icao24, str):
            icaos = [icao24.lower()]
        elif isinstance(icao24, Iterable):
            icaos = [x.lower() for x in icao24]
        else:
            icaos = None

        # for raw queries with bound filter
        if (type == "raw") and (bound is not None):
            print("** You are query raw messages with boundary.")
            print("** An ADS-B query will be performed to get ICAO codes.")

            # find out ICAO address first
            adsb_icaos = self.get_icaos(start=start, end=end, bound=bound)

            if icaos is not None:
                icaos = set(icaos).intersection(adsb_icaos)
            else:
                icaos = adsb_icaos

            print("** {} number of ICAOs.".format(len(adsb_icaos)))

            if len(icaos) == 0:
                print("** No data to be queried.")

            bound = None

        icao_filter = ""
        if icaos is not None:
            icao_filter = "AND icao24 in ({}) ".format(
                ",".join(["'" + x + "'" for x in icaos])
            )

        bound_filter = ""
        if bound is None:
            pass
        elif len(bound) != 4:
            raise RuntimeError("bound format must be [lat1, lon1, lat2, lon2]")
        else:
            lat1, lon1, lat2, lon2 = bound
            lat_min = min((lat1, lat2))
            lat_max = max((lat1, lat2))

            bound_filter += "AND lat>={} AND lat<={} ".format(lat_min, lat_max)

            if lon1 < lon2:
                bound_filter += "AND lon>={} AND lon<={} ".format(lon1, lon2)
            else:
                bound_filter += "AND lon>={} OR lon<={} ".format(lon1, lon2)

        extra_filter = ""
        if table == "rollcall_replies_data4":
            extra_filter += "AND message IS NOT NULL"

        limit_number = ""
        if limit:
            limit_number += "LIMIT {} ".format(limit)

        cmd = (
            "SELECT * FROM {} WHERE ".format(table)
            + "hour>={} ".format(hour_start)
            + "AND hour<={} ".format(hour_end)
            + "AND {}>={} ".format(time_col, ts_start)
            + "AND {}<={} ".format(time_col, ts_end)
            + icao_filter
            + bound_filter
            + extra_filter
            + limit_number
        )

        if limit is None and countfirst:
            # check how many records are related to the query
            count_cmd = cmd.replace("*", "COUNT(*)")
            print("* Obtaining details of the query...")
            logging.info("Sending count request: [" + count_cmd + "]")

            self.check_and_reconnect()
            output = self.shell("-q " + count_cmd)
            count = int(re.findall(r"\d+", output)[0])
            print("* OpenSky Impala: {} of records found.".format(count))

            if count == 0:
                print("* No record found.")
                return None
            elif count > 200000:
                print(
                    "* Too many records to download. "
                    + "You should consider exiting and re-partitioning the query."
                )
                time.sleep(5)

        # sending actual query
        print("* Fetching records...")
        logging.info("Sending query request: [" + cmd + "]")

        self.check_and_reconnect()
        output = self.shell("-q " + cmd)

        logging.info("Processing query result.")
        sio = StringIO()
        for i, line in enumerate(output.split("\n")):
            if "|" not in line:
                # keep only table rows
                continue
            if "hour" in line and i > 10:
                # skip header row, after first occurance
                continue
            new_line = re.sub(r" *\| *", ",", line)[1:-1]
            sio.write(new_line + "\n")

        if sio.tell() == 0:
            print("* No record found.")
            return None

        else:
            sio.seek(0)
            df = pd.read_csv(sio, dtype={"icao24": str})

            if "time" in df.columns.tolist():
                df = df.sort_values("time")
            elif "mintime" in df.columns.tolist():
                df = df.sort_values("mintime")

            print("* Records downloaded.")

            return df

    def get_icaos(self, start, end, bound):
        """Get ICAO address in a certain time period and geographic bound.

        Args:
            start (str): Start of time period with format YYYY-MM-DD HH:MM:SS
            end (str): End of time period with format YYYY-MM-DD HH:MM:SS
            bound (list): Filter latitude and longitude bound with format of [lat1, lon1, lat2, lon2], default to None
        Returns:
            list: List of ICAO addresses

        """
        ts_start = pd.Timestamp(start, tz="utc").timestamp()
        ts_end = pd.Timestamp(end, tz="utc").timestamp()

        hour_start = ts_start // 3600 * 3600
        hour_end = (ts_end // 3600 + 1) * 3600

        table = "state_vectors_data4"
        time_col = "time"

        bound_filter = ""

        lat1, lon1, lat2, lon2 = bound
        lat_min = min((lat1, lat2))
        lat_max = max((lat1, lat2))

        bound_filter += "AND lat>={} AND lat<={} ".format(lat_min, lat_max)

        if lon1 < lon2:
            bound_filter += "AND lon>={} AND lon<={} ".format(lon1, lon2)
        else:
            bound_filter += "AND lon>={} OR lon<={} ".format(lon1, lon2)

        cmd = (
            "SELECT DISTINCT icao24 FROM {} WHERE ".format(table)
            + "hour>={} ".format(hour_start)
            + "AND hour<={} ".format(hour_end)
            + "AND {}>={} ".format(time_col, ts_start)
            + "AND {}<={} ".format(time_col, ts_end)
            + bound_filter
        )

        logging.info("Sending query request: [" + cmd + "]")

        self.check_and_reconnect()
        output = self.shell("-q " + cmd)

        logging.info("Processing query result.")

        sio = StringIO()
        for i, line in enumerate(output.split("\n")):
            if "|" not in line:
                # keep only table rows
                continue
            if "hour" in line and i > 10:
                # skip header row, after first occurance
                continue
            new_line = re.sub(r" *\| *", ",", line)[1:-1]
            sio.write(new_line + "\n")

        if sio.tell() == 0:
            print("* No record found.")
            return []
        else:
            sio.seek(0)
            df = pd.read_csv(sio, dtype={"icao24": str})
            icaos = df.icao24.unique().tolist()
            return icaos

    def rawquery(self, cmd):
        """Perform a raw impala query.

        Args:
            cmd (str): Raw query command
        Returns:
            pandas.DataFrame: Impala query results

        """
        # sending actual query
        print("* Fetching records...")
        # logging.info("Sending query request: [" + cmd + "]")
        print("Sending query request: [" + cmd + "]")

        self.check_and_reconnect()
        output = self.shell("-q " + cmd)

        # logging.info("Processing query result.")
        print("Processing query result.")

        sio = StringIO()

        for i, line in enumerate(output.split("\n")):
            if "|" not in line:
                # keep only table rows
                continue
            if "hour" in line and i > 10:
                # skip header row, after first occurance
                continue
            new_line = re.sub(r" *\| *", ",", line)[1:-1]
            sio.write(new_line + "\n")

        if sio.tell() == 0:
            print("* No record found.")
            return None

        else:
            sio.seek(0)
            df = pd.read_csv(sio, dtype={"icao24": str})

            if "time" in df.columns.tolist():
                df = df.sort_values("time")
            elif "mintime" in df.columns.tolist():
                df = df.sort_values("mintime")

            print("* Records downloaded.")

            return df
