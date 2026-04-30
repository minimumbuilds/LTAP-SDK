"""LTAP observability records and emitter interface.

The specification defines two conformance levels:
  - Base:      one TickRecord per participant per tick
  - Replayable: Base record plus contenders list and rng_state
"""

from __future__ import annotations

import dataclasses
import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, List, Optional

from .models import ChannelId, ParticipantId

logger = logging.getLogger(__name__)


@dataclass
class TickRecord:
    """One observability record per participant per tick (Base conformance).

    When is_replayable is True the contenders and rng_state fields are
    also populated (Replayable conformance).
    """

    tick: int
    channel_id: ChannelId
    participant: ParticipantId
    raw_priority: float
    weighted_priority: float
    want_to_send: bool
    intent: str
    won: bool
    timed_out: bool
    ineligible: bool

    # Replayable conformance fields (None when not emitting Replayable records)
    contenders: Optional[List[ParticipantId]] = None
    rng_state: Optional[Any] = None

    def to_dict(self) -> dict:
        d = dataclasses.asdict(self)
        d.pop("rng_state", None)
        if self.rng_state is not None:
            d["rng_state"] = str(self.rng_state)
        return d


class ObservabilityEmitter(ABC):
    """Abstract sink for per-tick observability records."""

    @abstractmethod
    def emit(self, record: TickRecord) -> None: ...


class LoggingEmitter(ObservabilityEmitter):
    """Emits TickRecords to the Python logging system at DEBUG level."""

    def __init__(self, logger_name: str = "ltap.observability"):
        self._log = logging.getLogger(logger_name)

    def emit(self, record: TickRecord) -> None:
        self._log.debug(json.dumps(record.to_dict()))


class CallbackEmitter(ObservabilityEmitter):
    """Calls a user-provided callable for each TickRecord."""

    def __init__(self, callback):
        self._callback = callback

    def emit(self, record: TickRecord) -> None:
        self._callback(record)


class NullEmitter(ObservabilityEmitter):
    """Discards all records — useful for tests or when observability is not needed."""

    def emit(self, record: TickRecord) -> None:
        pass


class CollectingEmitter(ObservabilityEmitter):
    """Accumulates all TickRecords in memory — useful for testing."""

    def __init__(self) -> None:
        self.records: List[TickRecord] = []

    def emit(self, record: TickRecord) -> None:
        self.records.append(record)

    def clear(self) -> None:
        self.records.clear()
