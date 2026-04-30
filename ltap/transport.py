"""LTAP transport layer.

ParticipantConnector is the interface the Arbiter uses to communicate with
one participant.  Implementations can wrap anything: in-process callbacks,
HTTP, WebSockets, gRPC, etc.

InProcessConnector is the default implementation for single-process use;
it delegates directly to an LTAPParticipant's async methods.

Bundled bids (§4.2 / §4.6)
---------------------------
When a BusEvent carries a non-null bid_request, deliver_event must:
  1. Deliver the event to the participant (on_event).
  2. Call generate_bid with the embedded BidRequest.
  3. Return the resulting Bid.

The Arbiter stores the asyncio Task wrapping deliver_event and awaits it
(with BID_TIMEOUT) during Phase 2 of the following tick.  When bid_request
is null, deliver_event returns None and the return value is ignored.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional

from .models import Bid, BidRequest, BusEvent, ChannelId, TransmissionResponse

if TYPE_CHECKING:
    from .participant import LTAPParticipant


class ParticipantConnector(ABC):
    """Arbiter-facing communication interface for a single participant.

    The Arbiter applies its own BID_TIMEOUT / TRANSMISSION_TIMEOUT around
    calls to request_bid and signal_transmission; implementations do not need
    to add their own timeouts unless they have lower-level retry semantics.
    """

    @abstractmethod
    async def request_bid(self, request: BidRequest) -> Optional[Bid]:
        """Solicit a standalone bid (used when no bundled BidRequest is available).

        Returns a Bid, or None if the participant is unreachable.
        The Arbiter treats None identically to a timeout.
        """

    @abstractmethod
    async def signal_transmission(
        self, channel_id: ChannelId, tick: int
    ) -> Optional[TransmissionResponse]:
        """Notify the participant that it won and request its transmission.

        Returns a TransmissionResponse, or None on failure.
        """

    @abstractmethod
    async def deliver_event(self, event: BusEvent) -> Optional[Bid]:
        """Deliver a BusEvent to the participant.

        When event.bid_request is non-null the implementation MUST generate
        and return the participant's Bid for the next tick (bundled bid, §4.6).

        When event.bid_request is null, returns None; the return value is not
        used by the Arbiter.

        Exceptions are caught by the Arbiter; implementations must not rely on
        the caller handling exceptions.
        """


class InProcessConnector(ParticipantConnector):
    """Connector that calls participant methods directly within the same process."""

    def __init__(self, participant: "LTAPParticipant") -> None:
        self._participant = participant

    async def request_bid(self, request: BidRequest) -> Optional[Bid]:
        return await self._participant.generate_bid(request)

    async def signal_transmission(
        self, channel_id: ChannelId, tick: int
    ) -> Optional[TransmissionResponse]:
        return await self._participant.generate_transmission(channel_id)

    async def deliver_event(self, event: BusEvent) -> Optional[Bid]:
        await self._participant.on_event(event)
        if event.bid_request is not None:
            return await self._participant.generate_bid(event.bid_request)
        return None
