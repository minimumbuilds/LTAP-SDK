# LTAP SDK — Developer Guide

## What this is

Python SDK and reference Arbiter for the LLM Shared-Bus Turn Allocation Protocol (LTAP). Agents share a channel by bidding each tick; the Arbiter runs a 6-phase loop to pick a winner and broadcast the transmission.

## Running tests

```bash
pip install -e ".[dev]"
pytest                    # 63 tests, all async
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
  test_scenarios.py — multi-tick scenario suite (scripted agents, emergent properties)
```

## Key architecture decisions

**Tick loop is per-channel.** `create_channel()` spawns one asyncio Task per channel; `destroy_channel()` cancels it and drains the membership queue.

**Membership changes are deferred.** Register/deregister ops are enqueued and applied at the tick boundary (end of `_tick()`), so the running tick never sees a mid-tick roster change.

**Bundled bids (§4.6).** When a transmission occurs in Phase 6, the Arbiter embeds a `BidRequest` inside each `BusEvent` for participants that will be eligible next tick. The `deliver_event` task from Phase 6 doubles as the bid collector for Phase 2 of tick N+1. This halves the round-trips for active participants. `InProcessConnector.deliver_event` implements this — it must return a `Bid` when `event.bid_request` is non-null.

**Cooldown is post-win dampening, not a lockout.** The winner always stays eligible (and receives a bundled BidRequest). For the `COOLDOWN_TICKS` ticks after a win — derived at weighting time as `channel.tick - last_acted_tick <= COOLDOWN_TICKS`, no stored counter — the winner's bids are multiplied ×0.3 in Phase 3. `COOLDOWN_TICKS=0` disables dampening. `ineligible_ticks` is set only by the failure-streak path (§4.5).

**Direct-address bias.** If participant P was addressed by the previous transmission, its bid priority is multiplied ×2.0 in Phase 3. Multiplicative, so P's own priority signal is preserved — the earlier `max(p, 0.95)` floor caused persistent address loops in deployment (spec §4.3 design note).

**Safe default bid.** Any bid collection error (timeout, exception, None return, invalid field) results in `Bid(want_to_send=False, priority=0.0, intent="pass")`. Participants are never penalised for a bad bid — only for a bad transmission (failure streak).

## Adding a custom transport

Implement `ParticipantConnector` and call `arbiter.register_participant(channel_id, pid, connector)` directly instead of using `LTAPParticipant.register()`.

## Spec references

Source of truth is the LTAP v1.0 spec. Section references appear throughout the code:
- §3 — data model
- §4 — tick phases (4.1–4.6)
- §5 — membership (5.1 duplicate check, 5.4 query)
- §6 — observability conformance levels (Base / Replayable)
