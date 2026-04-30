"""LTAP participant base class.

LTAPParticipant is the SDK's primary extension point.  Subclass it and
implement generate_bid() and generate_transmission().  The class handles
registration/deregistration plumbing and wires up the InProcessConnector
automatically for single-process deployments.

For networked deployments implement ParticipantConnector directly and
call arbiter.register_participant() with your custom connector.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional

from .models import (
    Bid,
    BidRequest,
    BusEvent,
    ChannelId,
    MemberListResponse,
    ParticipantId,
    TransmissionResponse,
)
from .transport import InProcessConnector

if TYPE_CHECKING:
    from .arbiter import Arbiter


class LTAPParticipant(ABC):
    """Abstract base for an LTAP participant.

    Subclass this and implement:
      - generate_bid(request) → Bid
      - generate_transmission(channel_id) → TransmissionResponse

    Optionally override on_event(event) to react to bus events.

    The participant is identified by participant_id, which must be unique
    per channel (not globally unique — see §5.1 duplicate check).
    """

    def __init__(self, participant_id: ParticipantId) -> None:
        self.participant_id = participant_id

    # ------------------------------------------------------------------
    # Abstract protocol methods
    # ------------------------------------------------------------------

    @abstractmethod
    async def generate_bid(self, request: BidRequest) -> Bid:
        """Produce a Bid in response to a BidRequest from the Arbiter.

        Called during Phase 2 of each tick the participant is eligible for.
        Must complete within the Arbiter's BID_TIMEOUT; exceeding it causes
        the Arbiter to substitute the safe default bid.

        Return Bid(want_to_send=False, ...) to pass on this tick.
        """

    @abstractmethod
    async def generate_transmission(self, channel_id: ChannelId) -> TransmissionResponse:
        """Produce a TransmissionResponse after winning a tick.

        Called during Phase 5.  Must complete within the Arbiter's
        TRANSMISSION_TIMEOUT.  Returning an empty-content response or raising
        counts as a transmission failure.
        """

    # ------------------------------------------------------------------
    # Optional hook
    # ------------------------------------------------------------------

    async def on_event(self, event: BusEvent) -> None:
        """React to a BusEvent broadcast from the Arbiter.

        Called on every transmission and system event delivered to this
        participant.  The default implementation is a no-op.

        This method is called from the Arbiter's event-delivery task; any
        exceptions raised here are silently swallowed (best-effort delivery).
        """

    # ------------------------------------------------------------------
    # Registration helpers
    # ------------------------------------------------------------------

    async def register(
        self, arbiter: "Arbiter", channel_id: ChannelId
    ) -> MemberListResponse:
        """Register this participant on *channel_id* via *arbiter*.

        Blocks until the Arbiter processes the registration at the next
        tick boundary and returns a MemberListResponse.

        Raises:
            ChannelNotFoundError: if the channel does not exist.
            DuplicateRegistrationError: if already registered.
        """
        connector = InProcessConnector(self)
        return await arbiter.register_participant(
            channel_id, self.participant_id, connector
        )

    async def deregister(
        self, arbiter: "Arbiter", channel_id: ChannelId
    ) -> None:
        """Voluntarily deregister from *channel_id*.

        Blocks until the Arbiter processes the removal at the next tick
        boundary.

        Raises:
            ChannelNotFoundError: if the channel does not exist.
            ParticipantNotFoundError: if not registered on the channel.
        """
        await arbiter.deregister_participant(channel_id, self.participant_id)

    async def query_members(
        self, arbiter: "Arbiter", channel_id: ChannelId
    ) -> MemberListResponse:
        """Request a point-in-time member list snapshot from the Arbiter.

        Answered immediately from current Arbiter state; not deferred to
        a tick boundary (§5.4).

        Raises:
            ChannelNotFoundError: if the channel does not exist.
        """
        return await arbiter.query_members(channel_id)
