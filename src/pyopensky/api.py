from __future__ import annotations

from typing import Any, Callable, Iterable, TypeVar

from typing_extensions import Protocol

import pandas as pd

from .time import timelike

T = TypeVar("T")
ProgressbarType = Callable[[Iterable[T]], Iterable[T]]


class HasBounds(Protocol):
    @property
    def bounds(self) -> tuple[float, float, float, float]: ...


class OpenSkyDBAPI(Protocol):
    def flightlist(
        self,
        start: timelike,
        stop: timelike | None = None,
        *args: Any,  # more reasonable to be explicit about arguments
        departure_airport: str | list[str] | None = None,
        arrival_airport: str | list[str] | None = None,
        airport: str | list[str] | None = None,
        callsign: str | list[str] | None = None,
        icao24: str | list[str] | None = None,
        cached: bool = True,
        compress: bool = False,
        limit: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame | None: ...

    def history(
        self,
        start: timelike,
        stop: timelike | None = None,
        *args: Any,
        # date_delta: timedelta = timedelta(hours=1),
        callsign: str | list[str] | None = None,
        icao24: str | list[str] | None = None,
        serials: int | Iterable[int] | None = None,
        bounds: (
            str | HasBounds | tuple[float, float, float, float] | None
        ) = None,
        departure_airport: str | None = None,
        arrival_airport: str | None = None,
        airport: str | None = None,
        time_buffer: str | pd.Timedelta | None = None,
        cached: bool = True,
        compress: bool = False,
        limit: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame | None: ...

    def rawdata(
        self,
        start: timelike,
        stop: timelike | None = None,
        *args: Any,  # more reasonable to be explicit about arguments
        icao24: str | list[str] | None = None,
        serials: int | Iterable[int] | None = None,
        bounds: HasBounds | tuple[float, float, float, float] | None = None,
        callsign: str | list[str] | None = None,
        departure_airport: str | None = None,
        arrival_airport: str | None = None,
        airport: str | None = None,
        cached: bool = True,
        compress: bool = False,
        limit: int | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame | None: ...
