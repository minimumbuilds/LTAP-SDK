"""ltap — Python client SDK for the LLM Shared-Bus Turn Allocation Protocol.

Quickstart::

    import asyncio
    from ltap import Arbiter, LTAPParticipant, Bid, TransmissionResponse

    class MyAgent(LTAPParticipant):
        async def generate_bid(self, request):
            return Bid(want_to_send=True, priority=0.8)

        async def generate_transmission(self, channel_id):
            return TransmissionResponse(content="Hello, bus!")

    async def main():
        arbiter = Arbiter()
        arbiter.create_channel("general")

        agent = MyAgent("agent-1")
        member_list = await agent.register(arbiter, "general")

        await asyncio.sleep(1)   # let a few ticks run
        await agent.deregister(arbiter, "general")
        await arbiter.destroy_channel("general")

    asyncio.run(main())
"""

from .arbiter import Arbiter
from .exceptions import (
    ArbiterError,
    ChannelAlreadyExistsError,
    ChannelError,
    ChannelNotFoundError,
    DuplicateRegistrationError,
    LTAPError,
    ParticipantNotFoundError,
    RegistrationError,
    TransmissionError,
)
from .models import (
    BID_SCHEMA,
    Bid,
    BidRequest,
    BusEvent,
    Bus,
    Channel,
    ChannelId,
    MemberListResponse,
    Participant,
    ParticipantId,
    ParticipantTransmission,
    SystemEvent,
    TransmissionResponse,
)
from .observability import (
    CallbackEmitter,
    CollectingEmitter,
    LoggingEmitter,
    NullEmitter,
    ObservabilityEmitter,
    TickRecord,
)
from .participant import LTAPParticipant
from .transport import InProcessConnector, ParticipantConnector

__version__ = "0.1.0"

__all__ = [
    # Core
    "Arbiter",
    "LTAPParticipant",
    "ParticipantConnector",
    "InProcessConnector",
    # Models
    "BID_SCHEMA",
    "Bid",
    "BidRequest",
    "BusEvent",
    "Bus",
    "Channel",
    "ChannelId",
    "MemberListResponse",
    "Participant",
    "ParticipantId",
    "ParticipantTransmission",
    "SystemEvent",
    "TransmissionResponse",
    # Observability
    "ObservabilityEmitter",
    "TickRecord",
    "LoggingEmitter",
    "NullEmitter",
    "CallbackEmitter",
    "CollectingEmitter",
    # Exceptions
    "LTAPError",
    "ChannelError",
    "ChannelNotFoundError",
    "ChannelAlreadyExistsError",
    "RegistrationError",
    "DuplicateRegistrationError",
    "ParticipantNotFoundError",
    "TransmissionError",
    "ArbiterError",
]
