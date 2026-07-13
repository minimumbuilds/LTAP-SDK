"""Multi-tick scenario tests for the composed arbitration stack.

Scripted (non-LLM) participants drive the real Arbiter through realistic
multi-agent contention and the emergent properties are asserted from the
observability records — the properties that unit tests on _weight_bid
cannot see, and whose absence let two field failures ship:

- the pre-v1.0 hard post-win lockout (a sole willing speaker was muted), and
- the pre-v1.0 address floor (a zero-priority addressee was dragged in, and
  address loops could not be broken).

Each scenario notes which historical failure it would have caught.
Spec references: §4.3 (weighting), §4.4 (selection), §11 (field notes).
"""

from __future__ import annotations

import asyncio
import random
import unittest
from typing import Callable, Optional

from ltap import Arbiter, Bid, BidRequest, BusEvent, ChannelId, CollectingEmitter
from ltap.models import TransmissionResponse
from ltap.participant import LTAPParticipant

# Bid policy: (tick) -> (want_to_send, priority)
BidPolicy = Callable[[int], tuple[bool, float]]


class ScriptedAgent(LTAPParticipant):
    """Deterministic participant driven by a per-tick bid policy.

    ``addresses`` names the participant this agent addresses whenever it
    wins; None transmits unaddressed.  Self-win ticks are tracked so
    policies can implement client-side shaping (e.g. the chatroom's own
    cooldown dampening) keyed to the agent's last win.
    """

    def __init__(
        self,
        pid: str,
        policy: BidPolicy,
        *,
        addresses: Optional[str] = None,
    ) -> None:
        super().__init__(pid)
        self.policy = policy
        self.addresses = addresses
        self.last_win_tick: Optional[int] = None

    async def generate_bid(self, request: BidRequest) -> Bid:
        want, priority = self.policy(request.tick)
        return Bid(want_to_send=want, priority=priority)

    async def generate_transmission(
        self, channel_id: ChannelId
    ) -> TransmissionResponse:
        return TransmissionResponse(
            content=f"msg from {self.participant_id}",
            addressed_to=self.addresses,
        )

    async def on_event(self, event: BusEvent) -> None:
        if event.type == "transmission" and event.is_self:
            self.last_win_tick = event.tick


def winners_by_tick(emitter: CollectingEmitter) -> dict[int, Optional[str]]:
    """Map each observed tick to its winner (None = no transmission)."""
    ticks: dict[int, Optional[str]] = {}
    for r in emitter.records:
        ticks.setdefault(r.tick, None)
        if r.won:
            ticks[r.tick] = r.participant
    return ticks


def max_consecutive_wins(ticks: dict[int, Optional[str]]) -> int:
    longest = streak = 0
    prev: Optional[str] = None
    for t in sorted(ticks):
        w = ticks[t]
        if w is not None and w == prev:
            streak += 1
        elif w is not None:
            streak = 1
        else:
            streak = 0
        prev = w
        longest = max(longest, streak)
    return longest


class ScenarioBase(unittest.IsolatedAsyncioTestCase):
    """Shared arbiter/agent scaffolding for scenario runs."""

    async def run_scenario(
        self,
        agents: list[ScriptedAgent],
        *,
        duration: float = 0.5,
        channel: str = "scenario",
        **arbiter_kwargs,
    ) -> CollectingEmitter:
        random.seed(1337)  # deterministic tie-breaks
        emitter = CollectingEmitter()
        defaults = dict(
            cooldown_ticks=2,
            bid_timeout=0.2,
            transmission_timeout=1.0,
            observability_emitter=emitter,
            tick_interval=0.01,
        )
        defaults.update(arbiter_kwargs)
        arbiter = Arbiter(**defaults)
        arbiter.create_channel(channel)
        for a in agents:
            await a.register(arbiter, channel)

        await asyncio.sleep(duration)

        for a in agents:
            await a.deregister(arbiter, channel)
        await arbiter.destroy_channel(channel)
        return emitter


class TestAntiMonopoly(ScenarioBase):

    async def test_equal_bidders_never_win_consecutively(self):
        """Three equal 0.9 bidders: cooldown dampening (0.27 vs 0.9) makes
        back-to-back wins impossible while competitors contend (§4.3)."""
        agents = [
            ScriptedAgent(pid, lambda t: (True, 0.9)) for pid in ("a", "b", "c")
        ]
        emitter = await self.run_scenario(agents)
        # Sequential register() calls land at successive tick boundaries, so
        # the first registrant wins the warm-up ticks alone (correct
        # anti-lockout behaviour). Judge monopoly only from full occupancy.
        per_tick_count: dict[int, int] = {}
        for r in emitter.records:
            per_tick_count[r.tick] = per_tick_count.get(r.tick, 0) + 1
        full = {t for t, n in per_tick_count.items() if n == len(agents)}
        ticks = {t: w for t, w in winners_by_tick(emitter).items() if t in full}
        self.assertGreater(len(ticks), 10, "scenario too short to be meaningful")
        self.assertEqual(
            max_consecutive_wins(ticks),
            1,
            "a winner under cooldown must not beat undampened equal bidders",
        )


class TestAntiLockout(ScenarioBase):

    async def test_sole_willing_speaker_keeps_the_floor(self):
        """A sole eager speaker wins consecutive ticks: dampening is not a
        lockout.  REGRESSION: under the pre-v1.0 hard lockout the winner
        was ineligible for COOLDOWN_TICKS and the bus went silent (§11.2)."""
        agents = [
            ScriptedAgent("talker", lambda t: (True, 0.9)),
            ScriptedAgent("mute", lambda t: (False, 0.0)),
        ]
        emitter = await self.run_scenario(agents)
        ticks = winners_by_tick(emitter)
        self.assertGreaterEqual(
            max_consecutive_wins(ticks),
            3,
            "sole willing speaker must win consecutive ticks (dampened, not locked out)",
        )
        self.assertEqual(
            {w for w in ticks.values() if w}, {"talker"},
        )


class TestAddressBias(ScenarioBase):

    async def test_zero_priority_addressee_is_not_dragged_in(self):
        """An addressee bidding 0.0 never wins.  REGRESSION: the pre-v1.0
        max(p, 0.95) floor lifted any willing addressee to 0.95 and forced
        it into the conversation (§11.1)."""
        agents = [
            ScriptedAgent("caller", lambda t: (True, 0.9), addresses="reluctant"),
            ScriptedAgent("reluctant", lambda t: (True, 0.0)),
            ScriptedAgent("other", lambda t: (True, 0.5)),
        ]
        emitter = await self.run_scenario(agents)
        ticks = winners_by_tick(emitter)
        self.assertNotIn(
            "reluctant",
            set(ticks.values()),
            "0.0-priority addressee must never win under a multiplicative bias",
        )
        self.assertIn("caller", set(ticks.values()))

    async def test_addressee_signal_is_never_floored(self):
        """A low-priority addressee's weighted priority stays x2 of its raw
        bid — never jumping to a floor — so a strong uninvolved bidder
        out-competes it whenever undampened.  (A biased 0.4 CAN still win
        the ticks where every stronger bidder is inside its own cooldown
        window; that is intended §4.3 behaviour, not a defect.)
        REGRESSION: the pre-v1.0 floor lifted the addressee to 0.95 and it
        dominated every post-address tick (§11.1)."""
        agents = [
            ScriptedAgent("caller", lambda t: (True, 0.95), addresses="quiet"),
            ScriptedAgent("quiet", lambda t: (True, 0.2)),
            ScriptedAgent("keen", lambda t: (True, 0.9)),
        ]
        emitter = await self.run_scenario(agents)
        ticks = winners_by_tick(emitter)
        quiet_recs = [
            r for r in emitter.records if r.participant == "quiet" and not r.ineligible
        ]
        self.assertTrue(quiet_recs)
        for r in quiet_recs:
            self.assertLessEqual(
                r.weighted_priority,
                r.raw_priority * 2.0 + 1e-9,
                "addressee weighted priority must scale from its raw bid, not a floor",
            )
        wins = {p: sum(1 for w in ticks.values() if w == p) for p in ("quiet", "keen")}
        self.assertGreater(
            wins["keen"], wins["quiet"],
            f"a floored addressee would dominate; a biased one must not: {wins}",
        )

    async def test_strong_third_party_breaks_an_address_loop(self):
        """A and B mutually address each other (intended threading), but a
        third participant with a sufficiently strong bid breaks in.
        REGRESSION: under the floor the addressee always sat at 0.95 and a
        0.96 outsider was only tie-banded, never guaranteed (§11.1)."""
        # a/b: raw 0.45 -> 0.9 when addressed. c joins at tick 8 with 0.96:
        # outside the 0.05 tie band above 0.9, so c wins outright.
        agents = [
            ScriptedAgent("a", lambda t: (True, 0.45), addresses="b"),
            ScriptedAgent("b", lambda t: (True, 0.45), addresses="a"),
            ScriptedAgent("c", lambda t: (True, 0.96) if t >= 8 else (False, 0.0)),
        ]
        emitter = await self.run_scenario(agents)
        ticks = winners_by_tick(emitter)
        early = {t: w for t, w in ticks.items() if t < 8 and w}
        late = {t: w for t, w in ticks.items() if t >= 8 and w}
        self.assertTrue(early, "loop phase should produce transmissions")
        self.assertEqual(
            set(early.values()) - {"a", "b"},
            set(),
            "before c bids, a and b thread between themselves",
        )
        self.assertIn("c", set(late.values()), "strong outsider must break in")


class TestFairness(ScenarioBase):

    async def test_no_starvation_among_equal_bidders(self):
        """Four equal bidders over a long run: everyone wins, nobody
        dominates (§4.4 starvation resistance)."""
        pids = ("p1", "p2", "p3", "p4")
        agents = [ScriptedAgent(pid, lambda t: (True, 0.8)) for pid in pids]
        emitter = await self.run_scenario(agents, duration=0.8)
        ticks = winners_by_tick(emitter)
        wins = {pid: sum(1 for w in ticks.values() if w == pid) for pid in pids}
        total = sum(wins.values())
        self.assertGreater(total, 20, "scenario too short to be meaningful")
        for pid, count in wins.items():
            self.assertGreater(count, 0, f"{pid} starved over {total} wins")
            self.assertLessEqual(
                count / total, 0.5, f"{pid} dominated: {wins}"
            )


class TestSplitOwnership(ScenarioBase):
    """The chatroom configuration (§11.3/§11.4): cooldown shaping owned by
    the client, address bias owned by the arbiter at the field-tuned 1.5."""

    @staticmethod
    def _self_dampening_agent(pid: str, base: float, **kw) -> ScriptedAgent:
        agent: ScriptedAgent

        def policy(tick: int) -> tuple[bool, float]:
            lw = agent.last_win_tick
            if lw is not None and tick - lw <= 2:
                return True, base * 0.3  # client-side cooldown shaping
            return True, base

        agent = ScriptedAgent(pid, policy, **kw)
        return agent

    async def test_client_owned_cooldown_is_applied_exactly_once(self):
        """With dampening_factor=1.0, the arbiter passes the client-shaped
        priority through unchanged (weighted == raw when unaddressed), and
        the address bias applies exactly x1.5.  REGRESSION: both mechanics
        double-applied when client shaping met the arbiter's defaults."""
        x = self._self_dampening_agent("x", 0.9, addresses="y")
        y = ScriptedAgent("y", lambda t: (True, 0.5))
        emitter = await self.run_scenario(
            [x, y], dampening_factor=1.0, address_bias=1.5
        )
        ticks = winners_by_tick(emitter)
        self.assertIn("x", set(ticks.values()))
        self.assertIn("y", set(ticks.values()), "y must win x's cooldown ticks")

        # Skip x's first win: y's registration lands one tick boundary after
        # x's (sequential register() calls), so §4.5 validly strips the
        # addressed_to from x's first transmission — no bias exists yet.
        x_win_ticks = sorted(t for t, w in ticks.items() if w == "x")[1:]
        recs = {(r.tick, r.participant): r for r in emitter.records}
        checked_dampening = checked_bias = False
        for t in x_win_ticks:
            nxt_x = recs.get((t + 1, "x"))
            nxt_y = recs.get((t + 1, "y"))
            if nxt_x is not None and not nxt_x.ineligible:
                # Client submitted 0.27; arbiter must NOT dampen again.
                self.assertAlmostEqual(nxt_x.raw_priority, 0.27, places=5)
                self.assertAlmostEqual(
                    nxt_x.weighted_priority, 0.27, places=5,
                    msg="arbiter re-dampened a client-shaped bid",
                )
                checked_dampening = True
            if nxt_y is not None and not nxt_y.ineligible:
                # y was addressed by x's win: exactly raw x 1.5.
                self.assertAlmostEqual(
                    nxt_y.weighted_priority, nxt_y.raw_priority * 1.5, places=5
                )
                checked_bias = True
        self.assertTrue(checked_dampening and checked_bias,
                        "scenario never exercised the post-win tick")


class TestDoubleDampeningPitfall(ScenarioBase):

    async def test_stacked_dampeners_mute_a_sole_speaker(self):
        """DOCUMENTED PITFALL (§11.3): client-side shaping stacked on the
        arbiter's default dampener nets 0.9 x0.3 x0.3 = 0.081 < 0.1 — the
        eligibility threshold silences the only willing speaker for the
        whole cooldown window.  This test pins the failure mode the
        one-owner-per-mechanic rule exists to prevent."""
        solo = TestSplitOwnership._self_dampening_agent("solo", 0.9)
        emitter = await self.run_scenario([solo])  # arbiter defaults: x0.3
        ticks = winners_by_tick(emitter)
        win_ticks = sorted(t for t, w in ticks.items() if w == "solo")
        self.assertTrue(win_ticks)
        muted_after_win = [
            t for t in win_ticks if (t + 1) in ticks and ticks[t + 1] is None
        ]
        self.assertTrue(
            muted_after_win,
            "stacked dampeners must produce silent post-win ticks "
            "(if this fails, the pitfall no longer reproduces — update §11.3)",
        )


if __name__ == "__main__":
    unittest.main()
