"""LTAP protocol data model — all structured types defined in the specification."""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

if TYPE_CHECKING:
    pass

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

ParticipantId = str
ChannelId = str
LogEntry = Union["ParticipantTransmission", "SystemEvent"]

# ---------------------------------------------------------------------------
# Normative Bid JSON Schema (§3.3.1)
# Deployments MAY extend this to restrict `intent` to their IntentTag enum.
# ---------------------------------------------------------------------------

BID_SCHEMA: Dict[str, Any] = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["want_to_send", "priority", "intent"],
    "additionalProperties": False,
    "properties": {
        "want_to_send": {"type": "boolean"},
        "priority":     {"type": "number", "minimum": 0.0, "maximum": 1.0},
        "intent":       {"type": "string"},
    },
}


# ---------------------------------------------------------------------------
# Core protocol structures
# ---------------------------------------------------------------------------


@dataclass
class Participant:
    """Per-channel arbitration state for one participant (§3.1).

    cooldown was removed in spec v1.0; post-transmission backoff is now
    implemented via ineligible_ticks set in Phase 6.
    """

    id: ParticipantId
    ineligible_ticks: int = 0
    failure_streak: int = 0
    last_acted_tick: int = 0


@dataclass
class BidRequest:
    tick: int
    channel_id: ChannelId
    participant_id: ParticipantId
    intent_version: str


@dataclass
class Bid:
    want_to_send: bool
    priority: float
    intent: str = "pass"

    def to_dict(self) -> dict:
        return {
            "want_to_send": self.want_to_send,
            "priority": self.priority,
            "intent": self.intent,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Bid":
        return cls(
            want_to_send=d["want_to_send"],
            priority=d["priority"],
            intent=d.get("intent", "pass"),
        )


@dataclass
class TransmissionResponse:
    content: str
    addressed_to: Optional[ParticipantId] = None

    def to_dict(self) -> dict:
        return {"content": self.content, "addressed_to": self.addressed_to}

    @classmethod
    def from_dict(cls, d: dict) -> "TransmissionResponse":
        return cls(content=d["content"], addressed_to=d.get("addressed_to"))


# ---------------------------------------------------------------------------
# Log entries
# ---------------------------------------------------------------------------


@dataclass
class ParticipantTransmission:
    type: str = "participant"
    tick: int = 0
    sender: ParticipantId = ""
    content: str = ""
    addressed_to: Optional[ParticipantId] = None


@dataclass
class SystemEvent:
    type: str = "system"
    tick: int = 0
    content: str = ""


# ---------------------------------------------------------------------------
# Bus events (broadcast to participants)
# ---------------------------------------------------------------------------


@dataclass
class BusEvent:
    """Broadcast message from the Arbiter to a participant (§3.6).

    bid_request is non-null when the receiving participant will be eligible
    to bid in the next tick.  The participant's response to this BusEvent
    serves as its bid for that tick (bundled bid, §4.2 / §4.6).
    """

    type: str                               # "transmission" | "system"
    tick: int
    channel_id: ChannelId
    sender: Optional[ParticipantId]
    content: str
    addressed_to: Optional[ParticipantId]
    is_self: bool
    bid_request: Optional[BidRequest] = None   # non-null → bundled bid solicitation

    def to_dict(self) -> dict:
        d = dataclasses.asdict(self)
        return d


# ---------------------------------------------------------------------------
# Snapshot / query responses
# ---------------------------------------------------------------------------


@dataclass
class MemberListResponse:
    channel_id: ChannelId
    tick: int
    participants: List[ParticipantId] = field(default_factory=list)

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)


# ---------------------------------------------------------------------------
# Channel and Bus (Arbiter-managed state)
# ---------------------------------------------------------------------------


@dataclass
class Channel:
    id: ChannelId
    participants: dict = field(default_factory=dict)   # ParticipantId → Participant
    log: list = field(default_factory=list)             # List[LogEntry]
    tick: int = 0


@dataclass
class Bus:
    channels: dict = field(default_factory=dict)        # ChannelId → Channel
