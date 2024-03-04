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
        stop: None | timelike = None,
        *args: Any,  # more reasonable to be explicit about arguments
        departure_airport: None | str | list[str] = None,
        arrival_airport: None | str | list[str] = None,
        airport: None | str | list[str] = None,
        callsign: None | str | list[str] = None,
        icao24: None | str | list[str] = None,
        cached: bool = True,
        compress: bool = False,
        limit: None | int = None,
        **kwargs: Any,
    ) -> None | pd.DataFrame: ...

    def history(
        self,
        start: timelike,
        stop: None | timelike = None,
        *args: Any,
        # date_delta: timedelta = timedelta(hours=1),
        callsign: None | str | list[str] = None,
        icao24: None | str | list[str] = None,
        serials: None | int | Iterable[int] = None,
        bounds: None
        | str
        | HasBounds
        | tuple[float, float, float, float] = None,
        departure_airport: None | str = None,
        arrival_airport: None | str = None,
        airport: None | str = None,
        time_buffer: None | str | pd.Timedelta = None,
        cached: bool = True,
        compress: bool = False,
        limit: None | int = None,
        **kwargs: Any,
    ) -> None | pd.DataFrame: ...

    def rawdata(
        self,
        start: timelike,
        stop: None | timelike = None,
        *args: Any,  # more reasonable to be explicit about arguments
        icao24: None | str | list[str] = None,
        serials: None | int | Iterable[int] = None,
        bounds: None | HasBounds | tuple[float, float, float, float] = None,
        callsign: None | str | list[str] = None,
        departure_airport: None | str = None,
        arrival_airport: None | str = None,
        airport: None | str = None,
        cached: bool = True,
        compress: bool = False,
        limit: None | int = None,
        **kwargs: Any,
    ) -> None | pd.DataFrame: ...
