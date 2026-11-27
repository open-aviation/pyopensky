"""Experimental SQLAlchemy models for additional OpenSky tables.

These models deliberately live outside :mod:`pyopensky.schema` until their
upstream schemas and names are stable.
"""

from __future__ import annotations

from sqlalchemy.orm import Mapped, mapped_column

import pandas as pd

from .schema import (
    AddressString,
    Base,
    CallsignString,
    Sensors,
    UTCTimestampFloat,
)


class Df18Data4(Base):
    """Tentative filter model for the flat DF18 compatibility view.

    It contains stable/useful columns identified during the first analysis, not
    the full experimental schema.
    :meth:`pyopensky.trino.Trino.df18_messages` issues ``SELECT *`` so downloads
    still include every column exposed by the compatibility view.
    """

    __tablename__ = "df18_data4"

    sensors: Mapped[Sensors]
    rawmsg: Mapped[str]
    mintime: Mapped[pd.Timestamp] = mapped_column(UTCTimestampFloat)
    maxtime: Mapped[pd.Timestamp] = mapped_column(UTCTimestampFloat)
    msgcount: Mapped[int]

    address: Mapped[str] = mapped_column(AddressString)
    addresstype: Mapped[str | None]
    cf: Mapped[int]
    imf: Mapped[bool | None]
    msgkind: Mapped[str]
    formattypecode: Mapped[int | None]
    mepayload: Mapped[str | None]

    lat: Mapped[float | None]
    lon: Mapped[float | None]
    alt: Mapped[float | None]
    alttype: Mapped[str | None]
    surface: Mapped[bool | None]
    groundspeed: Mapped[float | None]
    mingroundspeed: Mapped[float | None]
    maxgroundspeed: Mapped[float | None]
    heading: Mapped[float | None]
    groundtrackangle: Mapped[float | None]
    airspeed: Mapped[float | None]
    verticalrate: Mapped[float | None]

    callsign: Mapped[str | None] = mapped_column(CallsignString)
    emittercategory: Mapped[int | None]
    adsbversion: Mapped[int | None]
    servicevolumeid: Mapped[int | None]

    # The table has aggregate row grain, so this is only an ORM identity hint.
    hour: Mapped[pd.Timestamp] = mapped_column(primary_key=True)
