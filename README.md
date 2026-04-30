# LTAP SDK

Python SDK and Arbiter for the **LLM Shared-Bus Turn Allocation Protocol (LTAP)** — a coordination protocol that lets multiple LLM agents share a conversation channel by bidding for the right to transmit each turn.

## How it works

Each channel runs a continuous tick loop with six phases:

| Phase | Action |
|-------|--------|
| 1 | Decrement post-win cooldown counters |
| 2 | Collect bids from eligible participants |
| 3 | Weight bids (apply direct-address bias) |
| 4 | Select winner (highest weighted priority; random tiebreak) |
| 5 | Signal winner, receive transmission |
| 6 | Broadcast event to all participants; piggyback next bid request |

Participants that win a tick are held ineligible for `cooldown_ticks` ticks. Participants that fail repeatedly are temporarily suspended.

## Install

```bash
pip install ltap-sdk          # from PyPI (once published)
# or from source:
pip install -e ".[dev]"
```

No runtime dependencies — stdlib only. Requires Python 3.10+.

## Quickstart

```python
import asyncio
from ltap import Arbiter, LTAPParticipant, Bid, BidRequest, ChannelId, TransmissionResponse

class MyAgent(LTAPParticipant):
    async def generate_bid(self, request: BidRequest) -> Bid:
        return Bid(want_to_send=True, priority=0.8, intent="speak")

    async def generate_transmission(self, channel_id: ChannelId) -> TransmissionResponse:
        return TransmissionResponse(content="Hello, bus!")

    async def on_event(self, event) -> None:
        print(f"tick={event.tick} sender={event.sender}: {event.content}")

async def main():
    arbiter = Arbiter(tick_interval=0.05)
    arbiter.create_channel("general")

    agent = MyAgent("agent-1")
    await agent.register(arbiter, "general")

    await asyncio.sleep(1.0)   # let ticks run

    await agent.deregister(arbiter, "general")
    await arbiter.destroy_channel("general")

asyncio.run(main())
```

See [`examples/simple_example.py`](examples/simple_example.py) for a three-agent demo with dynamic priorities and direct addressing.

## Core API

### `Arbiter`

```python
Arbiter(
    cooldown_ticks=1,           # ticks a winner sits out after transmitting
    bid_timeout=5.0,            # seconds to wait for a bid before substituting default
    transmission_timeout=60.0,  # seconds to wait for a transmission
    max_consecutive_failures=2, # failures before temporary suspension
    intent_version="1.0",
    observability_emitter=None, # defaults to LoggingEmitter
    tick_interval=0.0,          # seconds between ticks (0 = yield only)
)
```

| Method | Description |
|--------|-------------|
| `create_channel(channel_id)` | Create a channel and start its tick loop |
| `await destroy_channel(channel_id)` | Stop the loop and deregister all participants |
| `await register_participant(channel_id, participant_id, connector)` | Low-level registration with a custom connector |
| `await deregister_participant(channel_id, participant_id)` | Remove a participant |
| `await query_members(channel_id)` | Snapshot of current members |
| `.channels` | List of active channel IDs |

### `LTAPParticipant`

Subclass this and implement the two abstract methods:

```python
class LTAPParticipant(ABC):
    async def generate_bid(self, request: BidRequest) -> Bid: ...
    async def generate_transmission(self, channel_id: ChannelId) -> TransmissionResponse: ...
    async def on_event(self, event: BusEvent) -> None: ...  # optional

    async def register(self, arbiter, channel_id) -> MemberListResponse: ...
    async def deregister(self, arbiter, channel_id) -> None: ...
    async def query_members(self, arbiter, channel_id) -> MemberListResponse: ...
```

### `Bid`

```python
Bid(
    want_to_send=True,   # False = pass this tick
    priority=0.8,        # 0.0–1.0; clamped by Arbiter
    intent="speak",      # arbitrary string tag
)
```

A participant directly addressed by the previous transmission receives a priority floor of `0.95` regardless of its submitted priority.

### `TransmissionResponse`

```python
TransmissionResponse(
    content="...",            # must be non-empty
    addressed_to="agent-2",   # optional; directs next-tick priority boost
)
```

## Observability

Pass an emitter to `Arbiter(observability_emitter=...)`:

| Emitter | Behaviour |
|---------|-----------|
| `LoggingEmitter` | Logs each `TickRecord` as JSON at DEBUG level (default) |
| `NullEmitter` | Discards all records |
| `CallbackEmitter(fn)` | Calls `fn(record)` for each tick |
| `CollectingEmitter` | Stores records in `.records` list — good for tests |

`TickRecord` fields: `tick`, `channel_id`, `participant`, `raw_priority`, `weighted_priority`, `want_to_send`, `intent`, `won`, `timed_out`, `ineligible`, `contenders`, `rng_state`.

## Custom transport

For networked deployments implement `ParticipantConnector` and call `arbiter.register_participant()` directly:

```python
class MyConnector(ParticipantConnector):
    async def request_bid(self, request: BidRequest) -> Optional[Bid]: ...
    async def signal_transmission(self, channel_id, tick) -> Optional[TransmissionResponse]: ...
    async def deliver_event(self, event: BusEvent) -> Optional[Bid]: ...
```

`deliver_event` must return a `Bid` when `event.bid_request` is non-null (bundled bid optimisation, §4.6) and `None` otherwise.

## Running tests

```bash
pip install -e ".[dev]"
pytest
```

41 tests cover the full tick lifecycle, bid weighting, membership queue, cooldown, failure streaks, observability, and error cases.

## License

MIT
