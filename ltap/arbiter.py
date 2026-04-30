"""LTAP Arbiter — the neutral tick-loop controller.

The Arbiter owns every channel's tick loop.  It is architecturally
separate from the participant set and never submits bids or transmissions.

Protocol reference: §3.8, §4, §5, §6 (LTAP v1.0 spec).

Bundled bid flow (§4.2 / §4.6)
--------------------------------
When tick N produces a transmission the Arbiter embeds a BidRequest inside
the BusEvent delivered in Phase 6.  The participant's response to that event
IS its bid for tick N+1.  The Arbiter stores the asyncio Task that wraps each
deliver_event call and awaits it (with BID_TIMEOUT) at Phase 2 of tick N+1.
Only participants who did NOT receive a bundled BidRequest — new joiners and
participants who were ineligible during tick N — get a standalone BidRequest.
"""

from __future__ import annotations

import asyncio
import logging
import math
import random
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from .exceptions import (
    ArbiterError,
    ChannelAlreadyExistsError,
    ChannelNotFoundError,
    DuplicateRegistrationError,
    ParticipantNotFoundError,
)
from .models import (
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
from .observability import LoggingEmitter, ObservabilityEmitter, TickRecord
from .transport import ParticipantConnector

log = logging.getLogger(__name__)

_SAFE_DEFAULT_BID = Bid(want_to_send=False, priority=0.0, intent="pass")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


@dataclass
class _WeightedBid:
    participant_id: ParticipantId
    raw_bid: Bid
    weighted_priority: float
    timed_out: bool


@dataclass
class _MembershipOp:
    op: str                                    # "register" | "deregister"
    participant_id: ParticipantId
    connector: Optional[ParticipantConnector] = None
    future: Optional[asyncio.Future] = None


# ---------------------------------------------------------------------------
# Arbiter
# ---------------------------------------------------------------------------


class Arbiter:
    """Neutral arbiter that owns the tick loop for all channels on this bus.

    Parameters mirror the spec §3.8 Arbiter struct.  Reference values are
    used as defaults.

    Usage::

        arbiter = Arbiter()
        arbiter.create_channel("general")

        member_list = await participant.register(arbiter, "general")

        # The tick loop runs in the background once create_channel is called.
        # Call arbiter.destroy_channel(...) when done.
    """

    def __init__(
        self,
        *,
        cooldown_ticks: int = 1,
        bid_timeout: float = 5.0,
        transmission_timeout: float = 60.0,
        max_consecutive_failures: int = 2,
        intent_version: str = "1.0",
        observability_emitter: Optional[ObservabilityEmitter] = None,
        tick_interval: float = 0.0,
    ) -> None:
        if cooldown_ticks < 0:
            raise ValueError("cooldown_ticks must be >= 0")
        if max_consecutive_failures < 0:
            raise ValueError("max_consecutive_failures must be >= 0")

        self._bus = Bus()
        self.COOLDOWN_TICKS = cooldown_ticks
        self.BID_TIMEOUT = bid_timeout
        self.TRANSMISSION_TIMEOUT = transmission_timeout
        self.MAX_CONSECUTIVE_FAILURES = max_consecutive_failures
        self.intent_version = intent_version
        self._observability: ObservabilityEmitter = (
            observability_emitter or LoggingEmitter()
        )
        self._tick_interval = tick_interval

        # Per-channel runtime state
        self._connectors: Dict[ChannelId, Dict[ParticipantId, ParticipantConnector]] = {}
        self._membership_queues: Dict[ChannelId, asyncio.Queue] = {}
        self._channel_tasks: Dict[ChannelId, asyncio.Task] = {}
        self._stop_events: Dict[ChannelId, asyncio.Event] = {}

        # Bundled bid tasks carried from Phase 6 of tick N into Phase 2 of tick N+1.
        # Keys: channel_id → { participant_id → asyncio.Task[Optional[Bid]] }
        self._bundled_bids: Dict[ChannelId, Dict[ParticipantId, asyncio.Task]] = {}

    # ------------------------------------------------------------------
    # Public channel API
    # ------------------------------------------------------------------

    def create_channel(self, channel_id: ChannelId) -> Channel:
        """Create a new channel and start its tick loop.

        Raises:
            ChannelAlreadyExistsError: if the channel already exists.
            RuntimeError: if called outside a running event loop.
        """
        if channel_id in self._bus.channels:
            raise ChannelAlreadyExistsError(
                f"Channel '{channel_id}' already exists"
            )

        channel = Channel(id=channel_id)
        self._bus.channels[channel_id] = channel
        self._connectors[channel_id] = {}
        self._membership_queues[channel_id] = asyncio.Queue()
        stop_event = asyncio.Event()
        self._stop_events[channel_id] = stop_event

        task = asyncio.get_running_loop().create_task(
            self._run_channel(channel_id),
            name=f"ltap-channel-{channel_id}",
        )
        self._channel_tasks[channel_id] = task
        log.info("Channel '%s' created", channel_id)
        return channel

    async def destroy_channel(self, channel_id: ChannelId) -> None:
        """Destroy a channel and deregister all remaining participants.

        Raises:
            ChannelNotFoundError: if the channel does not exist.
        """
        if channel_id not in self._bus.channels:
            raise ChannelNotFoundError(f"Channel '{channel_id}' not found")

        self._stop_events[channel_id].set()
        task = self._channel_tasks.get(channel_id)
        if task and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

        channel = self._bus.channels.get(channel_id)
        if channel:
            for pid in sorted(channel.participants.keys()):
                await self._apply_deregistration(channel, pid, future=None)

        self._teardown_channel(channel_id)
        log.info("Channel '%s' destroyed", channel_id)

    @property
    def channels(self) -> List[ChannelId]:
        """Currently active channel IDs."""
        return list(self._bus.channels.keys())

    # ------------------------------------------------------------------
    # Public membership API
    # ------------------------------------------------------------------

    async def register_participant(
        self,
        channel_id: ChannelId,
        participant_id: ParticipantId,
        connector: ParticipantConnector,
    ) -> MemberListResponse:
        """Queue a registration and block until processed at the next tick boundary.

        Raises:
            ChannelNotFoundError: if the channel does not exist.
            DuplicateRegistrationError: if already registered on this channel.
        """
        if channel_id not in self._bus.channels:
            raise ChannelNotFoundError(f"Channel '{channel_id}' not found")

        future: asyncio.Future = asyncio.get_running_loop().create_future()
        op = _MembershipOp(
            op="register",
            participant_id=participant_id,
            connector=connector,
            future=future,
        )
        await self._membership_queues[channel_id].put(op)
        return await future

    async def deregister_participant(
        self,
        channel_id: ChannelId,
        participant_id: ParticipantId,
    ) -> None:
        """Queue a voluntary deregistration and block until processed.

        Raises:
            ChannelNotFoundError: if the channel does not exist.
            ParticipantNotFoundError: if not registered on this channel.
        """
        if channel_id not in self._bus.channels:
            raise ChannelNotFoundError(f"Channel '{channel_id}' not found")

        future: asyncio.Future = asyncio.get_running_loop().create_future()
        op = _MembershipOp(
            op="deregister",
            participant_id=participant_id,
            future=future,
        )
        await self._membership_queues[channel_id].put(op)
        await future

    async def query_members(self, channel_id: ChannelId) -> MemberListResponse:
        """Return a point-in-time snapshot of the channel's participant list (§5.4).

        Answered immediately from current state; not deferred to a tick boundary.

        Raises:
            ChannelNotFoundError: if the channel does not exist.
        """
        if channel_id not in self._bus.channels:
            raise ChannelNotFoundError(f"Channel '{channel_id}' not found")
        channel = self._bus.channels[channel_id]
        return MemberListResponse(
            channel_id=channel_id,
            tick=channel.tick,
            participants=list(channel.participants.keys()),
        )

    # ------------------------------------------------------------------
    # Channel tick loop
    # ------------------------------------------------------------------

    async def _run_channel(self, channel_id: ChannelId) -> None:
        channel = self._bus.channels[channel_id]
        stop_event = self._stop_events[channel_id]

        while not stop_event.is_set():
            try:
                await self._tick(channel)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception(
                    "Unexpected error in tick loop for channel '%s'", channel_id
                )

            if self._tick_interval > 0:
                await asyncio.sleep(self._tick_interval)
            else:
                await asyncio.sleep(0)  # yield so other tasks can run

    async def _tick(self, channel: Channel) -> None:
        """Execute one complete tick cycle for the given channel."""

        # ----------------------------------------------------------------
        # Phase 1 — Decrement counters (§4.1)
        # ----------------------------------------------------------------
        for p in channel.participants.values():
            p.ineligible_ticks = max(0, p.ineligible_ticks - 1)

        # ----------------------------------------------------------------
        # Phase 2 — Collect bids (§4.2)
        # Bundled bids (from Phase 6 of the previous tick) are awaited here.
        # Standalone BidRequests are issued for any eligible participant that
        # did not receive a bundled BidRequest last tick.
        # ----------------------------------------------------------------
        eligible_ids = [
            pid
            for pid, p in channel.participants.items()
            if p.ineligible_ticks == 0
        ]
        ineligible_ids = [
            pid
            for pid, p in channel.participants.items()
            if p.ineligible_ticks > 0
        ]

        # Consume bundled bid tasks left by the previous Phase 6.
        bundled = self._bundled_bids.pop(channel.id, {})
        # Discard tasks for participants who are no longer eligible (e.g. became
        # ineligible at the tick boundary due to a failure streak).
        bundled = {pid: t for pid, t in bundled.items() if pid in set(eligible_ids)}

        raw_bids: Dict[ParticipantId, Bid] = {}
        timed_out_flags: Dict[ParticipantId, bool] = {}

        if eligible_ids:
            tasks: Dict[ParticipantId, asyncio.Task] = {}
            for pid in eligible_ids:
                if pid in bundled:
                    # The event-delivery task from Phase 6 already called
                    # generate_bid; we just await its result.
                    tasks[pid] = asyncio.get_running_loop().create_task(
                        self._await_bundled_bid(bundled[pid])
                    )
                else:
                    connector = self._connectors[channel.id][pid]
                    bid_request = BidRequest(
                        tick=channel.tick,
                        channel_id=channel.id,
                        participant_id=pid,
                        intent_version=self.intent_version,
                    )
                    tasks[pid] = asyncio.get_running_loop().create_task(
                        self._collect_bid(connector, bid_request)
                    )

            results = await asyncio.gather(*tasks.values(), return_exceptions=True)
            for pid, result in zip(tasks.keys(), results):
                if isinstance(result, Exception):
                    raw_bids[pid] = _SAFE_DEFAULT_BID
                    timed_out_flags[pid] = True
                else:
                    bid, did_timeout = result
                    raw_bids[pid] = bid
                    timed_out_flags[pid] = did_timeout

        # ----------------------------------------------------------------
        # Phase 3 — Weight bids (§4.3)
        # Cooldown dampening removed; only direct-address bias + clamp remain.
        # ----------------------------------------------------------------
        most_recent_tx = self._most_recent_transmission(channel)
        weighted_bids: List[_WeightedBid] = []

        for pid in eligible_ids:
            raw = raw_bids.get(pid, _SAFE_DEFAULT_BID)
            wp = self._weight_bid(raw, pid, most_recent_tx)
            weighted_bids.append(
                _WeightedBid(
                    participant_id=pid,
                    raw_bid=raw,
                    weighted_priority=wp,
                    timed_out=timed_out_flags.get(pid, False),
                )
            )

        # ----------------------------------------------------------------
        # Phase 4 — Winner selection (§4.4)
        # ----------------------------------------------------------------
        eligible_weighted = [
            wb
            for wb in weighted_bids
            if wb.raw_bid.want_to_send and wb.weighted_priority > 0.1
        ]

        winner_id: Optional[ParticipantId] = None
        contenders: List[ParticipantId] = []
        rng_state: Any = None

        if eligible_weighted:
            top = max(wb.weighted_priority for wb in eligible_weighted)
            contender_bids = [
                wb for wb in eligible_weighted if wb.weighted_priority >= top - 0.05
            ]
            contenders = [wb.participant_id for wb in contender_bids]
            rng_state = random.getstate()
            winner_id = random.choice(contenders)

        # ----------------------------------------------------------------
        # Phase 5 — Signal and receive (§4.5)
        # ----------------------------------------------------------------
        valid_transmission: Optional[TransmissionResponse] = None

        if winner_id:
            connector = self._connectors[channel.id][winner_id]
            try:
                raw_response = await asyncio.wait_for(
                    connector.signal_transmission(channel.id, channel.tick),
                    timeout=self.TRANSMISSION_TIMEOUT,
                )
            except asyncio.TimeoutError:
                raw_response = None
            except Exception:
                log.debug(
                    "Transmission signal raised for '%s'", winner_id, exc_info=True
                )
                raw_response = None

            valid_transmission = self._validate_transmission(
                raw_response, channel, winner_id
            )

            if valid_transmission is None:
                p = channel.participants[winner_id]
                p.failure_streak += 1
                if p.failure_streak >= self.MAX_CONSECUTIVE_FAILURES:
                    p.ineligible_ticks = self.COOLDOWN_TICKS + 1
                    p.failure_streak = 0
                winner_id = None
            else:
                channel.participants[winner_id].failure_streak = 0

        # ----------------------------------------------------------------
        # Phase 6 — Broadcast events + bundled bid preparation (§4.6)
        # ----------------------------------------------------------------
        if valid_transmission is not None and winner_id is not None:
            # Log append precedes broadcast (invariant §7 #9)
            log_entry = ParticipantTransmission(
                tick=channel.tick,
                sender=winner_id,
                content=valid_transmission.content,
                addressed_to=valid_transmission.addressed_to,
            )
            channel.log.append(log_entry)

            # Apply post-transmission winner state BEFORE computing eligibility.
            # COOLDOWN_TICKS controls post-win suppression:
            #   0  → winner eligible immediately (gets a bundled BidRequest)
            #   1+ → winner sits out that many ticks (bid_request=null)
            # The +1 offset compensates for Phase 1's pre-decrement next tick.
            w = channel.participants[winner_id]
            w.last_acted_tick = channel.tick
            if self.COOLDOWN_TICKS > 0:
                w.ineligible_ticks = self.COOLDOWN_TICKS + 1
            w.failure_streak = 0

            # Pre-compute next-tick eligibility for each current participant.
            # A participant is eligible next tick when
            # max(0, ineligible_ticks - 1) == 0  after Phase 1 decrements.
            # With COOLDOWN_TICKS=0 the winner's post_decrement is 0 so it is
            # eligible and receives a bundled BidRequest; with COOLDOWN_TICKS>=1
            # post_decrement>=1 so it is excluded automatically — no special
            # pid==winner_id guard is needed.
            next_bundled: Dict[ParticipantId, asyncio.Task] = {}
            snapshot_connectors = dict(self._connectors[channel.id])

            for pid, conn in snapshot_connectors.items():
                post_decrement = max(0, channel.participants[pid].ineligible_ticks - 1)
                eligible_next = post_decrement == 0

                bid_req_for_event: Optional[BidRequest] = None
                if eligible_next:
                    bid_req_for_event = BidRequest(
                        tick=channel.tick + 1,
                        channel_id=channel.id,
                        participant_id=pid,
                        intent_version=self.intent_version,
                    )

                event = BusEvent(
                    type="transmission",
                    tick=channel.tick,
                    channel_id=channel.id,
                    sender=winner_id,
                    content=valid_transmission.content,
                    addressed_to=valid_transmission.addressed_to,
                    is_self=(pid == winner_id),
                    bid_request=bid_req_for_event,
                )

                delivery_task = asyncio.get_running_loop().create_task(
                    self._safe_deliver(conn, event)
                )
                if eligible_next:
                    next_bundled[pid] = delivery_task

            # Store bundled bid tasks for Phase 2 of the next tick.
            self._bundled_bids[channel.id] = next_bundled

        # ----------------------------------------------------------------
        # Observability records (§6)
        # ----------------------------------------------------------------
        self._emit_tick_records(
            channel=channel,
            eligible_ids=eligible_ids,
            ineligible_ids=ineligible_ids,
            raw_bids=raw_bids,
            timed_out_flags=timed_out_flags,
            weighted_bids=weighted_bids,
            winner_id=winner_id,
            contenders=contenders,
            rng_state=rng_state,
        )

        # ----------------------------------------------------------------
        # Tick boundary — apply queued membership changes (§5)
        # ----------------------------------------------------------------
        await self._apply_membership_queue(channel)

        channel.tick += 1

    # ------------------------------------------------------------------
    # Phase 2 helpers
    # ------------------------------------------------------------------

    async def _collect_bid(
        self, connector: ParticipantConnector, request: BidRequest
    ) -> Tuple[Bid, bool]:
        """Issue a standalone BidRequest and return (bid, timed_out)."""
        try:
            raw = await asyncio.wait_for(
                connector.request_bid(request), timeout=self.BID_TIMEOUT
            )
            if raw is None:
                return _SAFE_DEFAULT_BID, True
            return self._validate_bid(raw), False
        except asyncio.TimeoutError:
            return _SAFE_DEFAULT_BID, True
        except Exception:
            log.debug("Bid collection error", exc_info=True)
            return _SAFE_DEFAULT_BID, True

    async def _await_bundled_bid(
        self, delivery_task: asyncio.Task
    ) -> Tuple[Bid, bool]:
        """Await a Phase-6 deliver_event task and extract the bundled Bid."""
        try:
            result: Optional[Bid] = await asyncio.wait_for(
                asyncio.shield(delivery_task), timeout=self.BID_TIMEOUT
            )
            if result is None:
                return _SAFE_DEFAULT_BID, True
            return self._validate_bid(result), False
        except asyncio.TimeoutError:
            return _SAFE_DEFAULT_BID, True
        except Exception:
            log.debug("Bundled bid await error", exc_info=True)
            return _SAFE_DEFAULT_BID, True

    # ------------------------------------------------------------------
    # Bid semantic validation (§4.2)
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_bid(bid: Bid) -> Bid:
        if not isinstance(bid.want_to_send, bool):
            return _SAFE_DEFAULT_BID

        try:
            p = float(bid.priority)
        except (TypeError, ValueError):
            return _SAFE_DEFAULT_BID
        if math.isnan(p) or math.isinf(p):
            return _SAFE_DEFAULT_BID

        p = max(0.0, min(1.0, p))
        intent = bid.intent if isinstance(bid.intent, str) else "pass"
        return Bid(want_to_send=bid.want_to_send, priority=p, intent=intent)

    # ------------------------------------------------------------------
    # Phase 3: deterministic bid weighting (§4.3)
    # Cooldown dampening removed; only direct-address bias + clamp.
    # ------------------------------------------------------------------

    @staticmethod
    def _weight_bid(
        bid: Bid,
        participant_id: ParticipantId,
        most_recent_tx: Optional[ParticipantTransmission],
    ) -> float:
        p = bid.priority

        # Step 1: direct-address bias
        if (
            most_recent_tx is not None
            and most_recent_tx.addressed_to == participant_id
        ):
            p = max(p, 0.95)

        # Step 2: clamp
        return max(0.0, min(1.0, p))

    # ------------------------------------------------------------------
    # Log helper: most recent ParticipantTransmission
    # ------------------------------------------------------------------

    @staticmethod
    def _most_recent_transmission(
        channel: Channel,
    ) -> Optional[ParticipantTransmission]:
        for entry in reversed(channel.log):
            if isinstance(entry, ParticipantTransmission):
                return entry
        return None

    # ------------------------------------------------------------------
    # Phase 5: TransmissionResponse validation (§4.5)
    # ------------------------------------------------------------------

    def _validate_transmission(
        self,
        response: Optional[TransmissionResponse],
        channel: Channel,
        sender_id: ParticipantId,
    ) -> Optional[TransmissionResponse]:
        if response is None:
            return None
        if not isinstance(response.content, str) or not response.content:
            return None

        addressed_to = response.addressed_to

        if addressed_to == sender_id:
            addressed_to = None

        if addressed_to is not None and addressed_to not in channel.participants:
            addressed_to = None

        return TransmissionResponse(content=response.content, addressed_to=addressed_to)

    # ------------------------------------------------------------------
    # Event delivery helper
    # ------------------------------------------------------------------

    @staticmethod
    async def _safe_deliver(
        connector: ParticipantConnector, event: BusEvent
    ) -> Optional[Bid]:
        """Deliver a BusEvent and return the participant's bid when bundled."""
        try:
            return await connector.deliver_event(event)
        except Exception:
            log.debug("Event delivery failure", exc_info=True)
            return None

    # ------------------------------------------------------------------
    # Observability (§6)
    # ------------------------------------------------------------------

    def _emit_tick_records(
        self,
        *,
        channel: Channel,
        eligible_ids: List[ParticipantId],
        ineligible_ids: List[ParticipantId],
        raw_bids: Dict[ParticipantId, Bid],
        timed_out_flags: Dict[ParticipantId, bool],
        weighted_bids: List[_WeightedBid],
        winner_id: Optional[ParticipantId],
        contenders: List[ParticipantId],
        rng_state: Any,
    ) -> None:
        wb_map = {wb.participant_id: wb for wb in weighted_bids}

        for pid in eligible_ids:
            wb = wb_map.get(pid)
            raw = raw_bids.get(pid, _SAFE_DEFAULT_BID)
            record = TickRecord(
                tick=channel.tick,
                channel_id=channel.id,
                participant=pid,
                raw_priority=raw.priority,
                weighted_priority=wb.weighted_priority if wb else 0.0,
                want_to_send=raw.want_to_send,
                intent=raw.intent,
                won=(pid == winner_id),
                timed_out=timed_out_flags.get(pid, False),
                ineligible=False,
                contenders=contenders,
                rng_state=rng_state,
            )
            self._observability.emit(record)

        for pid in ineligible_ids:
            record = TickRecord(
                tick=channel.tick,
                channel_id=channel.id,
                participant=pid,
                raw_priority=0.0,
                weighted_priority=0.0,
                want_to_send=False,
                intent="pass",
                won=False,
                timed_out=False,
                ineligible=True,
                contenders=contenders,
                rng_state=rng_state,
            )
            self._observability.emit(record)

    # ------------------------------------------------------------------
    # Tick boundary: membership queue (§5)
    # ------------------------------------------------------------------

    async def _apply_membership_queue(self, channel: Channel) -> None:
        pending: List[_MembershipOp] = []
        queue = self._membership_queues[channel.id]
        while not queue.empty():
            pending.append(queue.get_nowait())

        deregistrations = [op for op in pending if op.op == "deregister"]
        registrations = [op for op in pending if op.op == "register"]

        for op in deregistrations:
            await self._apply_deregistration(channel, op.participant_id, op.future)

        for op in registrations:
            await self._apply_registration(
                channel, op.participant_id, op.connector, op.future
            )

    async def _apply_registration(
        self,
        channel: Channel,
        participant_id: ParticipantId,
        connector: Optional[ParticipantConnector],
        future: Optional[asyncio.Future],
    ) -> None:
        if participant_id in channel.participants:
            err = DuplicateRegistrationError(
                f"Participant '{participant_id}' already registered on "
                f"channel '{channel.id}'"
            )
            if future and not future.done():
                future.set_exception(err)
            return

        if connector is None:
            err = ArbiterError("Registration missing connector")
            if future and not future.done():
                future.set_exception(err)
            return

        channel.participants[participant_id] = Participant(id=participant_id)
        self._connectors[channel.id][participant_id] = connector

        channel.log.append(
            SystemEvent(
                tick=channel.tick,
                content=f"Participant '{participant_id}' joined channel '{channel.id}'",
            )
        )

        member_list = MemberListResponse(
            channel_id=channel.id,
            tick=channel.tick,
            participants=list(channel.participants.keys()),
        )
        if future and not future.done():
            future.set_result(member_list)

        event = BusEvent(
            type="system",
            tick=channel.tick,
            channel_id=channel.id,
            sender=None,
            content=f"Participant '{participant_id}' joined",
            addressed_to=None,
            is_self=False,
            bid_request=None,
        )
        for pid, conn in self._connectors[channel.id].items():
            if pid != participant_id:
                asyncio.get_running_loop().create_task(
                    self._safe_deliver(conn, event)
                )

        log.debug("Registered '%s' on channel '%s'", participant_id, channel.id)

    async def _apply_deregistration(
        self,
        channel: Channel,
        participant_id: ParticipantId,
        future: Optional[asyncio.Future],
    ) -> None:
        if participant_id not in channel.participants:
            err = ParticipantNotFoundError(
                f"Participant '{participant_id}' not found on channel '{channel.id}'"
            )
            if future and not future.done():
                future.set_exception(err)
            return

        del channel.participants[participant_id]
        del self._connectors[channel.id][participant_id]
        # Drop any pending bundled bid for this participant.
        self._bundled_bids.get(channel.id, {}).pop(participant_id, None)

        channel.log.append(
            SystemEvent(
                tick=channel.tick,
                content=f"Participant '{participant_id}' left channel '{channel.id}'",
            )
        )

        if future and not future.done():
            future.set_result(None)

        event = BusEvent(
            type="system",
            tick=channel.tick,
            channel_id=channel.id,
            sender=None,
            content=f"Participant '{participant_id}' left",
            addressed_to=None,
            is_self=False,
            bid_request=None,
        )
        for pid, conn in self._connectors[channel.id].items():
            asyncio.get_running_loop().create_task(self._safe_deliver(conn, event))

        log.debug("Deregistered '%s' from channel '%s'", participant_id, channel.id)

    # ------------------------------------------------------------------
    # Internal cleanup
    # ------------------------------------------------------------------

    def _teardown_channel(self, channel_id: ChannelId) -> None:
        self._bus.channels.pop(channel_id, None)
        self._connectors.pop(channel_id, None)
        self._membership_queues.pop(channel_id, None)
        self._stop_events.pop(channel_id, None)
        self._channel_tasks.pop(channel_id, None)
        self._bundled_bids.pop(channel_id, None)
