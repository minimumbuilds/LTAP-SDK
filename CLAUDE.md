# LTAP SDK — Developer Guide

## What this is

Python SDK and reference Arbiter for the LLM Shared-Bus Turn Allocation Protocol (LTAP). Agents share a channel by bidding each tick; the Arbiter runs a 6-phase loop to pick a winner and broadcast the transmission.

## Running tests

```bash
pip install -e ".[dev]"
pytest                    # 41 tests, all async
```

No runtime deps — stdlib only. Tests use `pytest-asyncio` with `asyncio_mode = "auto"`.

## Project layout

```
ltap/
  arbiter.py       — Arbiter class; owns the tick loop for all channels
  participant.py   — LTAPParticipant abstract base; subclass this
  transport.py     — ParticipantConnector ABC + InProcessConnector
  models.py        — All dataclasses: Bid, BusEvent, Channel, Bus, etc.
  observability.py — TickRecord + emitter hierarchy
  exceptions.py    — Exception hierarchy rooted at LTAPError
examples/
  simple_example.py
tests/
  test_arbiter.py
```

## Key architecture decisions

**Tick loop is per-channel.** `create_channel()` spawns one asyncio Task per channel; `destroy_channel()` cancels it and drains the membership queue.

**Membership changes are deferred.** Register/deregister ops are enqueued and applied at the tick boundary (end of `_tick()`), so the running tick never sees a mid-tick roster change.

**Bundled bids (§4.6).** When a transmission occurs in Phase 6, the Arbiter embeds a `BidRequest` inside each `BusEvent` for participants that will be eligible next tick. The `deliver_event` task from Phase 6 doubles as the bid collector for Phase 2 of tick N+1. This halves the round-trips for active participants. `InProcessConnector.deliver_event` implements this — it must return a `Bid` when `event.bid_request` is non-null.

**Cooldown is post-win suppression.** `COOLDOWN_TICKS=0` means the winner is immediately eligible again (and receives a bundled BidRequest). `COOLDOWN_TICKS>=1` sets `ineligible_ticks = COOLDOWN_TICKS + 1`; the +1 compensates for Phase 1's pre-decrement on the next tick.

**Direct-address bias.** If participant P was addressed by the previous transmission, its weighted priority is floored at 0.95 in Phase 3.

**Safe default bid.** Any bid collection error (timeout, exception, None return, invalid field) results in `Bid(want_to_send=False, priority=0.0, intent="pass")`. Participants are never penalised for a bad bid — only for a bad transmission (failure streak).

## Adding a custom transport

Implement `ParticipantConnector` and call `arbiter.register_participant(channel_id, pid, connector)` directly instead of using `LTAPParticipant.register()`.

## Spec references

Source of truth is the LTAP v1.0 spec. Section references appear throughout the code:
- §3 — data model
- §4 — tick phases (4.1–4.6)
- §5 — membership (5.1 duplicate check, 5.4 query)
- §6 — observability conformance levels (Base / Replayable)
