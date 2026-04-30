"""Simple LTAP example: three agents share a channel for 10 ticks.

Demonstrates:
  - Creating an Arbiter and a channel
  - Subclassing LTAPParticipant
  - Dynamic priority — agents vary their bids based on an internal counter
  - Direct addressing — Agent A occasionally addresses Agent B
  - Receiving and printing BusEvents
  - Graceful shutdown
"""

import asyncio
import logging

from ltap import (
    Arbiter,
    Bid,
    BidRequest,
    BusEvent,
    ChannelId,
    CollectingEmitter,
    LTAPParticipant,
    TransmissionResponse,
)

logging.basicConfig(level=logging.WARNING)


class CountingAgent(LTAPParticipant):
    """Bids with increasing priority each turn, wrapping at 1.0."""

    def __init__(self, name: str, step: float = 0.1, initial: float = 0.5) -> None:
        super().__init__(name)
        self._priority = initial
        self._step = step
        self._tx_count = 0
        self._events: list[BusEvent] = []
        self._address_next: str | None = None

    async def generate_bid(self, request: BidRequest) -> Bid:
        return Bid(want_to_send=True, priority=self._priority, intent="speak")

    async def generate_transmission(self, channel_id: ChannelId) -> TransmissionResponse:
        self._tx_count += 1
        self._priority = min(1.0, self._priority + self._step)
        msg = f"[{self.participant_id}] transmission #{self._tx_count}"
        resp = TransmissionResponse(content=msg, addressed_to=self._address_next)
        self._address_next = None
        return resp

    async def on_event(self, event: BusEvent) -> None:
        self._events.append(event)
        if event.type == "transmission":
            src = "(self)" if event.is_self else event.sender
            tgt = f" → {event.addressed_to}" if event.addressed_to else ""
            print(f"  tick={event.tick:3d} [{event.channel_id}] {src}{tgt}: {event.content}")
        else:
            print(f"  tick={event.tick:3d} [{event.channel_id}] SYSTEM: {event.content}")


async def main() -> None:
    emitter = CollectingEmitter()
    arbiter = Arbiter(
        cooldown_ticks=2,
        bid_timeout=5.0,
        transmission_timeout=10.0,
        max_consecutive_failures=2,
        observability_emitter=emitter,
        tick_interval=0.05,   # 50 ms between ticks — fast enough to see 10+
    )

    arbiter.create_channel("demo")

    alice = CountingAgent("alice", step=0.1, initial=0.4)
    bob   = CountingAgent("bob",   step=0.2, initial=0.6)
    carol = CountingAgent("carol", step=0.15, initial=0.5)

    print("=== Registering participants ===")
    for agent in (alice, bob, carol):
        ml = await agent.register(arbiter, "demo")
        print(f"  {agent.participant_id} joined; channel tick={ml.tick}, "
              f"members={ml.participants}")

    # Alice will address Bob after her first win
    alice._address_next = "bob"

    print("\n=== Running ticks (watch output below) ===")
    await asyncio.sleep(1.2)   # let the tick loop run for ~20+ ticks at 50 ms each

    print("\n=== Deregistering ===")
    for agent in (alice, bob, carol):
        await agent.deregister(arbiter, "demo")
        print(f"  {agent.participant_id} left")

    await arbiter.destroy_channel("demo")

    print("\n=== Transmission summary ===")
    for agent in (alice, bob, carol):
        wins = sum(1 for r in emitter.records if r.participant == agent.participant_id and r.won)
        print(f"  {agent.participant_id}: {agent._tx_count} transmissions, "
              f"{wins} tick wins recorded in observability")

    print("\n=== Sample observability records (last 5) ===")
    for rec in emitter.records[-5:]:
        print(
            f"  tick={rec.tick:3d} ch={rec.channel_id} p={rec.participant:6s} "
            f"raw={rec.raw_priority:.2f} wt={rec.weighted_priority:.2f} "
            f"won={rec.won} ineligible={rec.ineligible}"
        )


if __name__ == "__main__":
    asyncio.run(main())
