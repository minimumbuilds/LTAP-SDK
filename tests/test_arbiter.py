"""Tests for the LTAP Arbiter and core protocol invariants (spec v1.0 updated)."""

import asyncio
import unittest

from ltap import (
    BID_SCHEMA,
    Arbiter,
    Bid,
    BidRequest,
    BusEvent,
    ChannelId,
    CollectingEmitter,
    DuplicateRegistrationError,
    LTAPParticipant,
    MemberListResponse,
    NullEmitter,
    ParticipantNotFoundError,
    TransmissionResponse,
)
from ltap.arbiter import Arbiter, _SAFE_DEFAULT_BID
from ltap.models import Participant, ParticipantTransmission


# ---------------------------------------------------------------------------
# Helper participants
# ---------------------------------------------------------------------------


class _AlwaysBid(LTAPParticipant):
    def __init__(self, pid: str, priority: float = 0.9, content: str = "hello") -> None:
        super().__init__(pid)
        self.priority = priority
        self.content = content
        self.events: list[BusEvent] = []
        self.bids_generated: int = 0
        self.addressed_to: str | None = None

    async def generate_bid(self, request: BidRequest) -> Bid:
        self.bids_generated += 1
        return Bid(want_to_send=True, priority=self.priority)

    async def generate_transmission(self, channel_id: ChannelId) -> TransmissionResponse:
        return TransmissionResponse(content=self.content, addressed_to=self.addressed_to)

    async def on_event(self, event: BusEvent) -> None:
        self.events.append(event)


class _NeverBid(LTAPParticipant):
    async def generate_bid(self, request: BidRequest) -> Bid:
        return Bid(want_to_send=False, priority=0.0)

    async def generate_transmission(self, channel_id: ChannelId) -> TransmissionResponse:
        return TransmissionResponse(content="should not happen")


class _EmptyContent(LTAPParticipant):
    async def generate_bid(self, request: BidRequest) -> Bid:
        return Bid(want_to_send=True, priority=1.0)

    async def generate_transmission(self, channel_id: ChannelId) -> TransmissionResponse:
        return TransmissionResponse(content="")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_arbiter(**kwargs) -> Arbiter:
    defaults = dict(
        cooldown_ticks=1,
        bid_timeout=0.2,
        transmission_timeout=0.5,
        max_consecutive_failures=2,
        observability_emitter=NullEmitter(),
        tick_interval=0.0,
    )
    defaults.update(kwargs)
    return Arbiter(**defaults)


# ---------------------------------------------------------------------------
# Tests: Channel lifecycle
# ---------------------------------------------------------------------------


class TestChannelLifecycle(unittest.IsolatedAsyncioTestCase):

    async def test_create_and_destroy(self):
        arbiter = _make_arbiter()
        ch = arbiter.create_channel("c1")
        self.assertEqual(ch.id, "c1")
        self.assertIn("c1", arbiter.channels)
        await arbiter.destroy_channel("c1")
        self.assertNotIn("c1", arbiter.channels)

    async def test_duplicate_channel_raises(self):
        arbiter = _make_arbiter()
        arbiter.create_channel("dup")
        with self.assertRaises(Exception):
            arbiter.create_channel("dup")
        await arbiter.destroy_channel("dup")

    async def test_destroy_unknown_channel_raises(self):
        arbiter = _make_arbiter()
        with self.assertRaises(Exception):
            await arbiter.destroy_channel("nope")

    async def test_tick_advances_when_empty(self):
        arbiter = _make_arbiter(tick_interval=0.005)
        arbiter.create_channel("empty")
        await asyncio.sleep(0.05)
        ch = arbiter._bus.channels["empty"]
        self.assertGreater(ch.tick, 0)
        await arbiter.destroy_channel("empty")


# ---------------------------------------------------------------------------
# Tests: Registration
# ---------------------------------------------------------------------------


class TestRegistration(unittest.IsolatedAsyncioTestCase):

    async def test_register_returns_member_list(self):
        arbiter = _make_arbiter(tick_interval=0.005)
        arbiter.create_channel("reg")
        p = _AlwaysBid("p1")
        ml = await p.register(arbiter, "reg")
        self.assertIsInstance(ml, MemberListResponse)
        self.assertIn("p1", ml.participants)
        await arbiter.destroy_channel("reg")

    async def test_duplicate_registration_raises(self):
        arbiter = _make_arbiter(tick_interval=0.005)
        arbiter.create_channel("reg2")
        p = _AlwaysBid("dup")
        await p.register(arbiter, "reg2")
        p2 = _AlwaysBid("dup")
        with self.assertRaises(DuplicateRegistrationError):
            await p2.register(arbiter, "reg2")
        await arbiter.destroy_channel("reg2")

    async def test_deregister_removes_participant(self):
        arbiter = _make_arbiter(tick_interval=0.005)
        arbiter.create_channel("reg3")
        p = _AlwaysBid("todelete")
        await p.register(arbiter, "reg3")
        await p.deregister(arbiter, "reg3")
        ml = await arbiter.query_members("reg3")
        self.assertNotIn("todelete", ml.participants)
        await arbiter.destroy_channel("reg3")

    async def test_deregister_unknown_raises(self):
        arbiter = _make_arbiter(tick_interval=0.005)
        arbiter.create_channel("reg4")
        with self.assertRaises(ParticipantNotFoundError):
            await arbiter.deregister_participant("reg4", "ghost")
        await arbiter.destroy_channel("reg4")

    async def test_join_event_sent_to_existing(self):
        arbiter = _make_arbiter(tick_interval=0.005)
        arbiter.create_channel("evt")
        first = _AlwaysBid("first")
        await first.register(arbiter, "evt")

        second = _AlwaysBid("second")
        await second.register(arbiter, "evt")

        await asyncio.sleep(0.05)
        system_events = [e for e in first.events if e.type == "system"]
        self.assertTrue(
            any("second" in e.content for e in system_events),
            "first should have received a join event for second",
        )
        await arbiter.destroy_channel("evt")

    async def test_new_participant_has_no_cooldown_field(self):
        arbiter = _make_arbiter(tick_interval=0.005)
        arbiter.create_channel("nc")
        p = _AlwaysBid("newp")
        await p.register(arbiter, "nc")
        participant_record = arbiter._bus.channels["nc"].participants["newp"]
        self.assertFalse(hasattr(participant_record, "cooldown"),
                         "Participant must not have a cooldown field in updated spec")
        self.assertEqual(participant_record.ineligible_ticks, 0)
        await arbiter.destroy_channel("nc")


# ---------------------------------------------------------------------------
# Tests: Bid weighting (cooldown dampening removed)
# ---------------------------------------------------------------------------


class TestBidWeighting(unittest.IsolatedAsyncioTestCase):

    def test_no_address_no_bias(self):
        bid = Bid(want_to_send=True, priority=0.7)
        w = Arbiter._weight_bid(bid, "x", None)
        self.assertAlmostEqual(w, 0.7, places=5)

    def test_direct_address_bias_applied(self):
        most_recent = ParticipantTransmission(
            tick=1, sender="y", content="hi", addressed_to="x"
        )
        bid = Bid(want_to_send=True, priority=0.1)
        w = Arbiter._weight_bid(bid, "x", most_recent)
        self.assertAlmostEqual(w, 0.95, places=5)

    def test_direct_address_raises_low_priority(self):
        most_recent = ParticipantTransmission(
            tick=1, sender="y", content="hi", addressed_to="x"
        )
        bid = Bid(want_to_send=True, priority=0.0)
        w = Arbiter._weight_bid(bid, "x", most_recent)
        self.assertAlmostEqual(w, 0.95, places=5)

    def test_address_to_different_participant_no_effect(self):
        most_recent = ParticipantTransmission(
            tick=1, sender="y", content="hi", addressed_to="z"
        )
        bid = Bid(want_to_send=True, priority=0.5)
        w = Arbiter._weight_bid(bid, "x", most_recent)
        self.assertAlmostEqual(w, 0.5, places=5)

    def test_clamp_at_1(self):
        most_recent = ParticipantTransmission(
            tick=1, sender="y", content="hi", addressed_to="x"
        )
        bid = Bid(want_to_send=True, priority=1.0)
        w = Arbiter._weight_bid(bid, "x", most_recent)
        self.assertLessEqual(w, 1.0)

    def test_no_cooldown_dampening(self):
        """Cooldown dampening (×0.3) was removed from the spec; verify absence."""
        # Even with ineligible_ticks > 0 on the participant (shouldn't reach weighting),
        # the weight formula has no dampening factor.
        bid = Bid(want_to_send=True, priority=1.0)
        w = Arbiter._weight_bid(bid, "x", None)
        self.assertAlmostEqual(w, 1.0, places=5,
                               msg="No ×0.3 dampening should exist in updated spec")


# ---------------------------------------------------------------------------
# Tests: Bid validation
# ---------------------------------------------------------------------------


class TestBidValidation(unittest.IsolatedAsyncioTestCase):

    def test_non_boolean_want_to_send_gives_safe_default(self):
        bad = Bid(want_to_send="yes", priority=0.5)  # type: ignore
        result = Arbiter._validate_bid(bad)
        self.assertFalse(result.want_to_send)
        self.assertEqual(result.priority, 0.0)

    def test_nan_priority_gives_safe_default(self):
        bad = Bid(want_to_send=True, priority=float("nan"))
        result = Arbiter._validate_bid(bad)
        self.assertFalse(result.want_to_send)

    def test_inf_priority_gives_safe_default(self):
        bad = Bid(want_to_send=True, priority=float("inf"))
        result = Arbiter._validate_bid(bad)
        self.assertFalse(result.want_to_send)

    def test_out_of_range_priority_clamped(self):
        bid = Bid(want_to_send=True, priority=1.5)
        result = Arbiter._validate_bid(bid)
        self.assertEqual(result.priority, 1.0)

    def test_negative_priority_clamped(self):
        bid = Bid(want_to_send=True, priority=-0.5)
        result = Arbiter._validate_bid(bid)
        self.assertEqual(result.priority, 0.0)

    def test_unknown_intent_preserves_want_to_send_and_priority(self):
        bid = Bid(want_to_send=True, priority=0.5, intent="UNKNOWN_TAG")
        result = Arbiter._validate_bid(bid)
        self.assertTrue(result.want_to_send)
        self.assertAlmostEqual(result.priority, 0.5)


# ---------------------------------------------------------------------------
# Tests: Transmission validation
# ---------------------------------------------------------------------------


class TestTransmissionValidation(unittest.IsolatedAsyncioTestCase):

    def _ch(self):
        from ltap.models import Channel
        ch = Channel(id="test")
        ch.participants["a"] = Participant(id="a")
        ch.participants["b"] = Participant(id="b")
        return ch

    def _arb(self):
        return Arbiter(observability_emitter=NullEmitter())

    def test_none_is_failure(self):
        self.assertIsNone(self._arb()._validate_transmission(None, self._ch(), "a"))

    def test_empty_content_is_failure(self):
        resp = TransmissionResponse(content="")
        self.assertIsNone(self._arb()._validate_transmission(resp, self._ch(), "a"))

    def test_self_address_nulled(self):
        resp = TransmissionResponse(content="hi", addressed_to="a")
        result = self._arb()._validate_transmission(resp, self._ch(), "a")
        self.assertIsNotNone(result)
        self.assertIsNone(result.addressed_to)

    def test_unregistered_address_nulled(self):
        resp = TransmissionResponse(content="hi", addressed_to="ghost")
        result = self._arb()._validate_transmission(resp, self._ch(), "a")
        self.assertIsNotNone(result)
        self.assertIsNone(result.addressed_to)

    def test_valid_address_preserved(self):
        resp = TransmissionResponse(content="hi", addressed_to="b")
        result = self._arb()._validate_transmission(resp, self._ch(), "a")
        self.assertIsNotNone(result)
        self.assertEqual(result.addressed_to, "b")


# ---------------------------------------------------------------------------
# Tests: End-to-end behaviour
# ---------------------------------------------------------------------------


class TestEndToEnd(unittest.IsolatedAsyncioTestCase):

    async def test_single_participant_wins_eventually(self):
        emitter = CollectingEmitter()
        arbiter = Arbiter(
            cooldown_ticks=1,
            bid_timeout=0.2,
            transmission_timeout=1.0,
            max_consecutive_failures=3,
            observability_emitter=emitter,
            tick_interval=0.01,
        )
        arbiter.create_channel("solo")
        p = _AlwaysBid("solo-p", priority=0.9, content="solo message")
        await p.register(arbiter, "solo")

        await asyncio.sleep(0.2)

        wins = [r for r in emitter.records if r.won and r.participant == "solo-p"]
        self.assertGreater(len(wins), 0, "participant should have won at least once")

        await p.deregister(arbiter, "solo")
        await arbiter.destroy_channel("solo")

    async def test_never_bid_wins_nothing(self):
        emitter = CollectingEmitter()
        arbiter = Arbiter(
            cooldown_ticks=1,
            bid_timeout=0.1,
            transmission_timeout=1.0,
            observability_emitter=emitter,
            tick_interval=0.01,
        )
        arbiter.create_channel("silent")
        p = _NeverBid("quiet")
        await p.register(arbiter, "silent")

        await asyncio.sleep(0.15)

        wins = [r for r in emitter.records if r.won]
        self.assertEqual(len(wins), 0, "never-bidder should never win")

        await p.deregister(arbiter, "silent")
        await arbiter.destroy_channel("silent")

    async def test_winner_becomes_ineligible_after_win(self):
        """Phase 6 sets ineligible_ticks = COOLDOWN_TICKS+1; winner sits out that many ticks."""
        emitter = CollectingEmitter()
        arbiter = Arbiter(
            cooldown_ticks=2,
            bid_timeout=0.2,
            transmission_timeout=1.0,
            observability_emitter=emitter,
            tick_interval=0.01,
        )
        arbiter.create_channel("inelig")
        p = _AlwaysBid("w", priority=1.0)
        await p.register(arbiter, "inelig")

        await asyncio.sleep(0.15)

        # After any win, the next records for that participant should have
        # ineligible=True for COOLDOWN_TICKS consecutive ticks.
        recs = [r for r in emitter.records if r.participant == "w"]
        for i, rec in enumerate(recs):
            if rec.won:
                # The next COOLDOWN_TICKS records should be ineligible
                inelig_following = [
                    recs[j].ineligible
                    for j in range(i + 1, min(i + 1 + 2, len(recs)))
                ]
                if inelig_following:
                    self.assertTrue(
                        inelig_following[0],
                        "winner should be ineligible the tick immediately after winning",
                    )
                break

        await p.deregister(arbiter, "inelig")
        await arbiter.destroy_channel("inelig")

    async def test_multiple_participants_share_turns(self):
        emitter = CollectingEmitter()
        arbiter = Arbiter(
            cooldown_ticks=1,
            bid_timeout=0.2,
            transmission_timeout=1.0,
            observability_emitter=emitter,
            tick_interval=0.01,
        )
        arbiter.create_channel("multi")

        agents = [_AlwaysBid(f"agent-{i}", priority=0.8) for i in range(3)]
        for a in agents:
            await a.register(arbiter, "multi")

        await asyncio.sleep(0.5)

        all_wins = {r.participant for r in emitter.records if r.won}
        self.assertGreater(
            len(all_wins), 1,
            "with multiple equal-priority agents, more than one should win over time",
        )

        for a in agents:
            await a.deregister(arbiter, "multi")
        await arbiter.destroy_channel("multi")

    async def test_transmission_failure_increments_streak(self):
        arbiter = Arbiter(
            cooldown_ticks=1,
            bid_timeout=0.1,
            transmission_timeout=0.1,
            max_consecutive_failures=2,
            observability_emitter=NullEmitter(),
            tick_interval=0.01,
        )
        arbiter.create_channel("fail")
        p = _EmptyContent("failer")
        await p.register(arbiter, "fail")

        await asyncio.sleep(0.3)

        ml = await arbiter.query_members("fail")
        self.assertIn("failer", ml.participants)

        await p.deregister(arbiter, "fail")
        await arbiter.destroy_channel("fail")

    async def test_log_append_precedes_broadcast(self):
        """Invariant §7 #9: log append happens before event delivery."""
        log_lengths_at_event: list[int] = []

        class LogCheckAgent(_AlwaysBid):
            def __init__(self, arbiter_ref, channel_id, **kw):
                super().__init__(**kw)
                self._arb = arbiter_ref
                self._ch_id = channel_id

            async def on_event(self, event: BusEvent) -> None:
                if event.type == "transmission":
                    ch = self._arb._bus.channels.get(self._ch_id)
                    if ch:
                        log_lengths_at_event.append(len(ch.log))
                await super().on_event(event)

        arbiter = Arbiter(
            cooldown_ticks=1,
            bid_timeout=0.2,
            transmission_timeout=1.0,
            observability_emitter=NullEmitter(),
            tick_interval=0.01,
        )
        arbiter.create_channel("logcheck")
        agent = LogCheckAgent(
            arbiter, "logcheck", pid="a", priority=0.9, content="test"
        )
        await agent.register(arbiter, "logcheck")

        await asyncio.sleep(0.2)

        for log_len in log_lengths_at_event:
            self.assertGreater(log_len, 0, "log must have entries when event is delivered")

        await agent.deregister(arbiter, "logcheck")
        await arbiter.destroy_channel("logcheck")

    async def test_bundled_bid_request_in_bus_event(self):
        """Phase 6 must embed a BidRequest in the BusEvent for eligible participants."""
        bundled_requests: list[BidRequest] = []

        class BundleCapture(_AlwaysBid):
            async def on_event(self, event: BusEvent) -> None:
                if event.bid_request is not None:
                    bundled_requests.append(event.bid_request)
                await super().on_event(event)

        arbiter = Arbiter(
            cooldown_ticks=1,
            bid_timeout=0.3,
            transmission_timeout=1.0,
            observability_emitter=NullEmitter(),
            tick_interval=0.02,
        )
        arbiter.create_channel("bundle")
        # Two agents; the non-winner should receive a bundled BidRequest.
        a = BundleCapture("a", priority=0.9)
        b = BundleCapture("b", priority=0.5)
        await a.register(arbiter, "bundle")
        await b.register(arbiter, "bundle")

        await asyncio.sleep(0.4)

        self.assertGreater(
            len(bundled_requests), 0,
            "at least one BusEvent should carry a non-null bid_request",
        )
        for br in bundled_requests:
            self.assertEqual(br.channel_id, "bundle")
            self.assertIsInstance(br.tick, int)

        await a.deregister(arbiter, "bundle")
        await b.deregister(arbiter, "bundle")
        await arbiter.destroy_channel("bundle")

    async def test_winner_gets_null_bid_request_when_cooldown_nonzero(self):
        """With cooldown_ticks>=1 the winner's BusEvent has bid_request=null."""
        winner_bid_requests: list = []

        class WinnerCapture(_AlwaysBid):
            async def on_event(self, event: BusEvent) -> None:
                if event.is_self:
                    winner_bid_requests.append(event.bid_request)
                await super().on_event(event)

        arbiter = Arbiter(
            cooldown_ticks=1,
            bid_timeout=0.3,
            transmission_timeout=1.0,
            observability_emitter=NullEmitter(),
            tick_interval=0.02,
        )
        arbiter.create_channel("wbr")
        p = WinnerCapture("winner", priority=1.0)
        await p.register(arbiter, "wbr")

        await asyncio.sleep(0.3)

        self.assertGreater(len(winner_bid_requests), 0, "winner should have transmitted")
        for br in winner_bid_requests:
            self.assertIsNone(br, "winner gets bid_request=null when cooldown_ticks>=1")

        await p.deregister(arbiter, "wbr")
        await arbiter.destroy_channel("wbr")

    async def test_winner_gets_bid_request_when_cooldown_zero(self):
        """With cooldown_ticks=0 the winner receives a bundled BidRequest like any other participant."""
        winner_bid_requests: list = []

        class WinnerCapture(_AlwaysBid):
            async def on_event(self, event: BusEvent) -> None:
                if event.is_self:
                    winner_bid_requests.append(event.bid_request)
                await super().on_event(event)

        arbiter = Arbiter(
            cooldown_ticks=0,
            bid_timeout=0.3,
            transmission_timeout=1.0,
            observability_emitter=NullEmitter(),
            tick_interval=0.02,
        )
        arbiter.create_channel("wbr2")
        p = WinnerCapture("winner2", priority=1.0)
        await p.register(arbiter, "wbr2")

        await asyncio.sleep(0.3)

        self.assertGreater(len(winner_bid_requests), 0, "winner should have transmitted")
        non_null = [br for br in winner_bid_requests if br is not None]
        self.assertGreater(
            len(non_null), 0,
            "winner should receive a non-null bid_request when cooldown_ticks=0",
        )

    async def test_winner_eligible_immediately_when_cooldown_zero(self):
        """With cooldown_ticks=0 the winner can win back-to-back ticks."""
        emitter = CollectingEmitter()
        arbiter = Arbiter(
            cooldown_ticks=0,
            bid_timeout=0.2,
            transmission_timeout=1.0,
            observability_emitter=emitter,
            tick_interval=0.01,
        )
        arbiter.create_channel("nosup")
        p = _AlwaysBid("solo", priority=1.0)
        await p.register(arbiter, "nosup")

        await asyncio.sleep(0.3)

        recs = [r for r in emitter.records if r.participant == "solo"]
        wins = [r for r in recs if r.won]
        inelig = [r for r in recs if r.ineligible]

        self.assertGreater(len(wins), 1, "solo participant should win multiple ticks")
        self.assertEqual(len(inelig), 0, "no ineligible ticks with cooldown_ticks=0")

        win_ticks = {r.tick for r in wins}
        consecutive_found = any((t + 1) in win_ticks for t in win_ticks)
        self.assertTrue(consecutive_found, "back-to-back wins should be possible with cooldown_ticks=0")

        await p.deregister(arbiter, "nosup")
        await arbiter.destroy_channel("nosup")


# ---------------------------------------------------------------------------
# Tests: Query members
# ---------------------------------------------------------------------------


class TestQueryMembers(unittest.IsolatedAsyncioTestCase):

    async def test_query_returns_current_state(self):
        arbiter = _make_arbiter(tick_interval=0.005)
        arbiter.create_channel("q")
        a = _AlwaysBid("qa")
        b = _AlwaysBid("qb")
        await a.register(arbiter, "q")
        await b.register(arbiter, "q")

        ml = await arbiter.query_members("q")
        self.assertIn("qa", ml.participants)
        self.assertIn("qb", ml.participants)

        await a.deregister(arbiter, "q")
        ml2 = await arbiter.query_members("q")
        self.assertNotIn("qa", ml2.participants)
        self.assertIn("qb", ml2.participants)

        await b.deregister(arbiter, "q")
        await arbiter.destroy_channel("q")


# ---------------------------------------------------------------------------
# Tests: Bid schema constant
# ---------------------------------------------------------------------------


class TestBidSchema(unittest.TestCase):

    def test_bid_schema_exported(self):
        self.assertIn("properties", BID_SCHEMA)
        self.assertIn("want_to_send", BID_SCHEMA["properties"])
        self.assertIn("priority", BID_SCHEMA["properties"])
        self.assertIn("intent", BID_SCHEMA["properties"])

    def test_bid_schema_no_additional_properties(self):
        self.assertFalse(BID_SCHEMA.get("additionalProperties", True))

    def test_bid_schema_required_fields(self):
        self.assertEqual(
            set(BID_SCHEMA["required"]), {"want_to_send", "priority", "intent"}
        )


if __name__ == "__main__":
    unittest.main()
