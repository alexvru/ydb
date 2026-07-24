#include "interconnect_uring_engine.h"

#include "uring_recv_buffer_pool.h"
#include "uring_context.h" // for TUringContext::IsSupported() / SqThreadIdleMs

#include "v2_event_serializer.h"
#include "interconnect_direct_session.h"
#include "interconnect_uring_event_queue.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>

#include <ydb/library/actors/protos/interconnect.pb.h>

// Must be included AFTER YDB headers because linux/uapi headers pulled by
// liburing may define macros that clash with project headers.
#include <ydb/library/uring/liburing_linux.h>

#include <util/system/env.h>
#include <util/system/hp_timer.h>

#include <sys/socket.h>
#include <sys/uio.h>
#include <sys/timerfd.h>
#include <sys/eventfd.h>

#include <cerrno>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

namespace NActors {

    namespace {
        constexpr ui32 RingQueueDepth = 4096;
        constexpr unsigned CqeBatchSize = 64;
        constexpr size_t ReadBufferSize = 262144;
        constexpr size_t MinReadBufferSize = 65536;
        constexpr size_t WriteBufferSize = 262144;
        constexpr size_t MinWriteBufferSize = 65536;
        constexpr size_t MaxSpansPerWrite = 64;
        constexpr size_t SerializeWindowSize = 65536;
        constexpr size_t MinSerializeWindowSize = 4096;
        constexpr ui32 RebalanceTimerMs = 100; // ping every PingEveryNTicks; also drives MaybeOffload
        constexpr ui32 PingEveryNTicks = 20; // 20 * 100ms = 2s
        constexpr ui32 OffloadBusyThreshold = 700000; // ppm
        constexpr ui32 StealBusyThreshold = 300000; // ppm
    }

    class TUringEngine final : public IUringEngine {
        TActorSystem *ActorSystem = nullptr; // bound after construction via SetActorSystem()
        std::once_flag ActorSystemInitFlag;
        std::atomic_bool Stopping{false};

        enum class EMigrateState : ui8 {
            None = 0,
            Draining,
            HandedOff,
        };

        // Low 3 bits of the session pointer are used as an io_uring user_data op tag; heap allocation
        // alignment of this type is already >= 8 (actually 64 via base/members).
        struct TRegisteredSession : TEventDeserializer::IEventProcessor {
            std::atomic<ui32> OwnerShard;
            ui32 RingIdx;
            const TIntrusivePtr<NInterconnect::TStreamSocket> Socket;
            const TActorId SessionId;
            const std::function<void(TDisconnectReason)> OnDisconnectCallback;
            TActorSystem* const ActorSystem;
            TEventSerializer Serializer;
            TEventDeserializer Deserializer;
            TRcBuf ReadBuffer;
            bool Terminated = false;
            bool ReadPending = false;
            bool WritePending = false;
            bool UnregisterRequested = false;
            const bool SendPings;
            TRcBuf WriteBuffer;
            std::deque<TContiguousSpan> OutgoingSpans;
            iovec Iov[MaxSpansPerWrite];
            size_t IovLen = 0;
            size_t UnsentBytes = 0;

            EMigrateState MigrateState = EMigrateState::None;
            ui32 MigrateTargetShard = 0;
            ui32 MigrateSourceShard = 0;

            const std::shared_ptr<std::atomic<int64_t>> ClockSkew;
            const std::shared_ptr<std::atomic<uint64_t>> PingRTT;

            THashMap<TActorId, TIntrusivePtr<IReceiveCallback>> ReceiveCallbacks;
            NMonitoring::TDynamicCounters::TCounterPtr EventsReceived;

            TRegisteredSession(ui32 shardIdx, ui32 ringIdx, TIntrusivePtr<NInterconnect::TStreamSocket> socket,
                    TActorId sessionId, bool checksumming, TScopeId peerScopeId,
                    std::function<void(TDisconnectReason)> onDisconnectCallback, TActorSystem *actorSystem,
                    bool sendPings, std::shared_ptr<std::atomic<int64_t>> clockSkew,
                    std::shared_ptr<std::atomic<uint64_t>> pingRTT)
                : OwnerShard(shardIdx)
                , RingIdx(ringIdx)
                , Socket(std::move(socket))
                , SessionId(sessionId)
                , OnDisconnectCallback(std::move(onDisconnectCallback))
                , ActorSystem(actorSystem)
                , Serializer(checksumming)
                , Deserializer(peerScopeId)
                , SendPings(sendPings)
                , ClockSkew(std::move(clockSkew))
                , PingRTT(std::move(pingRTT))
            {}

            void Disconnect(TDisconnectReason reason) {
                OnDisconnectCallback(reason);
                Terminated = true;
            }

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // deserialization/receiving

            TMutableContiguousSpan GetReadSpan() {
                if (ReadBuffer.size() < MinReadBufferSize) {
                    ReadBuffer = TRcBuf::Uninitialized(ReadBufferSize);
                }
                return ReadBuffer.UnsafeGetContiguousSpanMut();
            }

            void ApplyBytesRead(size_t num) {
                TRcBuf chunk = {TRcBuf::Piece, ReadBuffer.data(), num, ReadBuffer};
                Deserializer.Push(std::move(chunk), this, SessionId);
                Y_ABORT_UNLESS(num <= ReadBuffer.size());
                const size_t remain = ReadBuffer.size() - num;
                ReadBuffer.TrimFront(remain - remain % 64); // make only this number of bytes remaining in buffer
            }

            void PushEvent(std::unique_ptr<IEventHandle> ev) override {
                if (const auto it = ReceiveCallbacks.find(ev->Recipient); it != ReceiveCallbacks.end()) {
                    it->second->Receive(ev.release());
                } else {
                    ActorSystem->Send(ev.release());
                }
                ++*EventsReceived;
            }

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // serialization/sending

            bool Serialize() {
                if (UnsentBytes >= MinSerializeWindowSize && !Serializer.HasOutOfBandTraffic()) {
                    return false;
                }

                Serializer.ResetCounters();

                while (UnsentBytes < SerializeWindowSize && OutgoingSpans.size() < MaxSpansPerWrite) {
                    if (WriteBuffer.size() < MinWriteBufferSize) { // (re)allocate write buffer
                        WriteBuffer = TRcBuf::Uninitialized(WriteBufferSize);
                    } else { // align write buffer to 64-byte boundary
                        WriteBuffer.TrimFront(WriteBuffer.size() - WriteBuffer.size() % 64);
                    }
                    const size_t numBytesProduced = Serializer.ProduceOutputStream(WriteBuffer, &OutgoingSpans,
                        SerializeWindowSize - UnsentBytes);

                    if (!numBytesProduced) {
                        break;
                    }
                    UnsentBytes += numBytesProduced;
                }

                return true;
            }

            bool PrepareIovec() {
                // Build the iovec WITHOUT consuming spans: writev may complete partially, so a span is only
                // dropped once the bytes it covers have actually been confirmed sent (see ApplyBytesWritten).
                IovLen = 0;
                for (const TContiguousSpan& span : OutgoingSpans) {
                    if (IovLen >= MaxSpansPerWrite) {
                        break;
                    }
                    Iov[IovLen++] = {
                        .iov_base = const_cast<char*>(span.data()),
                        .iov_len = span.size(),
                    };
                }
                return IovLen != 0;
            }

            void ApplyBytesWritten(size_t num, std::vector<ui64> *eventToWireTime) {
                // Advance past exactly the bytes the kernel accepted. A writev can be short (e.g. under
                // backpressure or on a real network), so drop only fully-sent spans and trim the span that
                // straddles the boundary; the rest stay queued and are retried by the next writev.
                for (size_t remaining = num; remaining && !OutgoingSpans.empty(); OutgoingSpans.pop_front()) {
                    if (TContiguousSpan& front = OutgoingSpans.front(); front.size() <= remaining) {
                        remaining -= front.size();
                    } else {
                        front = TContiguousSpan(front.data() + remaining, front.size() - remaining);
                        break;
                    }
                }

                Y_ABORT_UNLESS(num <= UnsentBytes, "num# %zu UnsentBytes# %zu", num, UnsentBytes);
                UnsentBytes -= num;

                Serializer.CommitProducedBytes(num, eventToWireTime);
            }

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // ping/clock skew management

            NHPTimer::STime PingRequestSentTimestamp = 0;
            NHPTimer::STime PingResponseSentTimestamp = 0;

            void SendPingRequest() {
                NActorsInterconnect::TSystemPayloadV2 systemRequest;
                auto *r = systemRequest.AddRequests();
                r->MutablePingRequest();
                Serializer.Push(systemRequest);
                PingRequestSentTimestamp = GetCycleCountFast();
            }

            void Process(NActorsInterconnect::TSystemPayloadV2& systemRequest) override {
                std::optional<NActorsInterconnect::TSystemPayloadV2> response;

                auto addRequest = [&] {
                    if (!response) {
                        response.emplace();
                    }
                    return response->AddRequests();
                };

                const NHPTimer::STime timestamp = GetCycleCountFast();
                const TInstant now = Now();

                auto calculateRoundTripTimeAndSkew = [&](auto& item, NHPTimer::STime sent) {
                    const ui64 rtt = NHPTimer::GetSeconds(timestamp - sent) * 1e6;
                    const i64 skew = item.GetWallClock() + rtt / 2 - now.MicroSeconds();
                    RegisterPingAndSkew(rtt, skew);
                };

                for (const auto& item : systemRequest.GetRequests()) {
                    switch (item.GetRequestCase()) {
                        case NActorsInterconnect::TSystemPayloadV2::TRequest::kPingRequest: {
                            // we have received PingRequest from the peer -- we have to remember when we got it, send
                            // the reply and wait for PingConfirm to make up our ClockSkew value
                            auto *pr = addRequest()->MutablePingResponse();
                            pr->SetWallClock(now.MicroSeconds());
                            PingResponseSentTimestamp = timestamp;
                            break;
                        }

                        case NActorsInterconnect::TSystemPayloadV2::TRequest::kPingResponse: {
                            calculateRoundTripTimeAndSkew(item.GetPingResponse(), PingRequestSentTimestamp);
                            PingRequestSentTimestamp = 0;

                            auto *pc = addRequest()->MutablePingConfirm();
                            pc->SetWallClock(now.MicroSeconds());
                            break;
                        }

                        case NActorsInterconnect::TSystemPayloadV2::TRequest::kPingConfirm:
                            calculateRoundTripTimeAndSkew(item.GetPingConfirm(), PingResponseSentTimestamp);
                            PingResponseSentTimestamp = 0;
                            break;

                        case NActorsInterconnect::TSystemPayloadV2::TRequest::REQUEST_NOT_SET:
                            break;
                    }
                }

                if (response) {
                    Serializer.Push(*response);
                }
            }

            ui64 PingValues[3] = {0, 0, 0};

            void RegisterPingAndSkew(ui64 pingUs, i64 skew) {
                ClockSkew->store(skew);

                // calculate worst ping over three last times
                PingValues[0] = PingValues[1];
                PingValues[1] = PingValues[2];
                PingValues[2] = pingUs;
                PingRTT->store(Max(PingValues[0], PingValues[1], PingValues[2]));
            }

            bool IsMigratable() const {
                return MigrateState == EMigrateState::None
                    && !UnregisterRequested
                    && !Terminated
                    && !WritePending
                    && UnsentBytes == 0
                    && !Serializer.IsTrafficPending();
            }
        };

        // In-process load signal consumed by rebalancing (not the 15s monitoring scrape path).
        struct TShardLoad {
            std::atomic<ui64> BusyNs{0};
            std::atomic<ui64> TotalNs{0};
            std::atomic<ui32> Sessions{0};
            std::atomic<ui64> Bytes{0};

            ui32 BusyFraction() const {
                const ui64 total = TotalNs.load(std::memory_order_relaxed);
                if (!total) {
                    return 0;
                }
                return BusyNs.load(std::memory_order_relaxed) * 1'000'000 / total;
            }
        };

        class TShard {
            enum EOperationType {
                kOpEvent = 1,
                kOpRead,
                kOpWrite,
                kOpTimer,
                kOpCancel,
            };
            static const ui64 kOpMask = (1 << 3) - 1;

            struct TRingSlot {
                io_uring Ring{};
                i64 ItemsToSubmit = 0;
            };

            TUringEngine* const Engine;
            const ui32 ShardIdx;
            TIncomingEventQueue IncomingEventQueue;
            std::thread Worker;

            const bool UseEventFdAsCQ;
            std::vector<TRingSlot> Rings;
            int EventFd = -1;
            ui64 EventFdReadBuffer = 0;
            int TimerFd = -1;
            char ReadTimerBuffer[256];
            ui32 TimerTicks = 0;
            std::atomic_bool WaitingForCQ{false};

            struct TSessionHash {
                size_t operator()(const std::unique_ptr<TRegisteredSession>& p) const { return THash<void*>{}(p.get()); }
                size_t operator()(const TRegisteredSession *p) const { return THash<void*>{}(p); }
            };

            struct TSessionEqual {
                using T = std::unique_ptr<TRegisteredSession>;
                bool operator()(const T& x, const T& y) const { return x == y; }
                bool operator()(const TRegisteredSession *x, const T& y) const { return x == y.get(); }
                bool operator()(const T& x, const TRegisteredSession *y) const { return x.get() == y; }
            };

            THashSet<std::unique_ptr<TRegisteredSession>, TSessionHash, TSessionEqual> Sessions;
            // conn -> target shard while OwnerShard still points here during handoff
            THashMap<ui64, ui32> MigratingOut;
            std::atomic_uint64_t NextRingIdx{0};

            NMonitoring::TDynamicCounters::TCounterPtr SessionsRegistered;
            NMonitoring::TDynamicCounters::TCounterPtr SessionsUnregistered;
            NMonitoring::TDynamicCounters::TCounterPtr EventsSent;
            NMonitoring::TDynamicCounters::TCounterPtr EventsReceived;
            NMonitoring::TDynamicCounters::TCounterPtr DirectReceiveCallbacksRegistered;
            NMonitoring::TDynamicCounters::TCounterPtr DirectReceiveCallbacksUnregistered;
            NMonitoring::TDynamicCounters::TCounterPtr BytesSent;
            NMonitoring::TDynamicCounters::TCounterPtr BytesCopied;
            NMonitoring::TDynamicCounters::TCounterPtr BytesAliased;
            NMonitoring::TDynamicCounters::TCounterPtr BytesReceived;
            NMonitoring::TDynamicCounters::TCounterPtr SQEAllocated;
            NMonitoring::TDynamicCounters::TCounterPtr SubmitCount;
            NMonitoring::TDynamicCounters::TCounterPtr CQEProcessed;
            NMonitoring::TDynamicCounters::TCounterPtr EventWakeups;
            NMonitoring::TDynamicCounters::TCounterPtr PushedAsFirst;
            NMonitoring::TDynamicCounters::TCounterPtr PushedTotal;
            NMonitoring::TDynamicCounters::TCounterPtr ReadUnavail;
            NMonitoring::TDynamicCounters::TCounterPtr WriteUnavail;
            NMonitoring::TDynamicCounters::TCounterPtr SessionsMigratedOut;
            NMonitoring::TDynamicCounters::TCounterPtr SessionsMigratedIn;

            NMonitoring::TDynamicCounters::TCounterPtr OtherTotalTime;
            NMonitoring::TDynamicCounters::TCounterPtr CompleteWaitTotalTime;
            NMonitoring::TDynamicCounters::TCounterPtr SubmitWaitTotalTime;
            NMonitoring::TDynamicCounters::TCounterPtr ApplyBytesReadTotalTime;
            NMonitoring::TDynamicCounters::TCounterPtr ApplyBytesWrittenTotalTime;
            NMonitoring::TDynamicCounters::TCounterPtr SerializeBufferTotalTime;
            NMonitoring::TDynamicCounters::TCounterPtr SerializeEventTotalTime;

            NMonitoring::THistogramPtr CommandDeliveryTime;
            NMonitoring::THistogramPtr EventToWireTime;
            NMonitoring::THistogramPtr CompletionWaitTime;
            NMonitoring::THistogramPtr CommandExecTime;
            NMonitoring::THistogramPtr SubmitExecTime;
            NMonitoring::THistogramPtr SerializeTime;
            NMonitoring::THistogramPtr CompletionsProcessedAtOnce;
            NMonitoring::THistogramPtr SubmissionsProcessedAtOnce;

            ui64 LastActivitySwitchTimestamp = 0;
            NMonitoring::TDynamicCounters::TCounterPtr *CurrentActivityTime = &OtherTotalTime;

            const double Freq = 1e9 * NHPTimer::GetSeconds(1); // nanoseconds per cycle

            std::vector<ui64> EventToWireTimeVec;

            TShardLoad& Load;

        private:
            class TActivityMeasure {
                TShard& Shard;
                NMonitoring::TDynamicCounters::TCounterPtr *PrevActivityTime;

            public:
                TActivityMeasure(TShard& shard, NMonitoring::TDynamicCounters::TCounterPtr *activityTime)
                    : Shard(shard)
                    , PrevActivityTime(std::exchange(shard.CurrentActivityTime, activityTime))
                {
                    **PrevActivityTime += UpdateTimestamp();
                }

                ~TActivityMeasure() {
                    const ui64 delta = UpdateTimestamp();
                    if (Shard.CurrentActivityTime) {
                        **Shard.CurrentActivityTime += delta;
                    }
                    Shard.CurrentActivityTime = PrevActivityTime;
                }

                ui64 UpdateTimestamp() {
                    const ui64 prevTimestamp = std::exchange(Shard.LastActivitySwitchTimestamp, GetCycleCountFast());
                    return (Shard.LastActivitySwitchTimestamp - prevTimestamp) * Shard.Freq;
                }
            };

#define ACTIVITY(NAME) if (TActivityMeasure __measure{*this, NAME}; false); else

            void InitRing(TRingSlot& slot, bool sqpoll, ui32 sqThreadIdleMs, TRingSlot *shareWith) {
                auto tryIt = [&](std::optional<ui32> sqThreadIdleMs) {
                    io_uring_params params{};
                    if (sqpoll) {
                        params.flags |= IORING_SETUP_SQPOLL;
                        if (sqThreadIdleMs) {
                            params.sq_thread_idle = *sqThreadIdleMs;
                        }
                    }
                    if (shareWith) {
                        params.flags |= IORING_SETUP_ATTACH_WQ;
                        params.wq_fd = shareWith->Ring.ring_fd;
                    }
                    return io_uring_queue_init_params(RingQueueDepth, &slot.Ring, &params) == 0;
                };
                if (!tryIt(sqThreadIdleMs) && !tryIt(std::nullopt)) {
                    Y_ABORT("failed to initialize ring");
                }
            }

            void UpdateParkState() {
                Load.Sessions.store(Sessions.size(), std::memory_order_relaxed);
            }

            void PublishLoadSample(ui64 busyDeltaNs, ui64 totalDeltaNs) {
                // Keep a short EWMA-like window by decaying prior samples and adding the latest slice.
                constexpr ui64 DecayNum = 3;
                constexpr ui64 DecayDen = 4;
                const ui64 prevBusy = Load.BusyNs.load(std::memory_order_relaxed);
                const ui64 prevTotal = Load.TotalNs.load(std::memory_order_relaxed);
                Load.BusyNs.store(prevBusy * DecayNum / DecayDen + busyDeltaNs, std::memory_order_relaxed);
                Load.TotalNs.store(prevTotal * DecayNum / DecayDen + totalDeltaNs, std::memory_order_relaxed);
                Load.Sessions.store(Sessions.size(), std::memory_order_relaxed);
            }

        public:
            static NMonitoring::IHistogramCollectorPtr TimeCollector() {
                return NMonitoring::ExponentialHistogram(22, 2, 1000);
            }

            TShard(TUringEngine* engine, ui32 shardIdx, const NMonitoring::TDynamicCounterPtr& shardCounters, bool sqpoll,
                    ui32 ringsPerShard, ui32 sqThreadIdleMs, TShardLoad& load, TShard *shareRingsWith)
#define COUNTER(NAME, DERIV) NAME(shardCounters->GetCounter(#NAME, DERIV))
                : Engine(engine)
                , ShardIdx(shardIdx)
                , UseEventFdAsCQ(ringsPerShard > 1)
                , COUNTER(SessionsRegistered, true)
                , COUNTER(SessionsUnregistered, true)
                , COUNTER(EventsSent, true)
                , COUNTER(EventsReceived, true)
                , COUNTER(DirectReceiveCallbacksRegistered, true)
                , COUNTER(DirectReceiveCallbacksUnregistered, true)
                , COUNTER(BytesSent, true)
                , COUNTER(BytesCopied, true)
                , COUNTER(BytesAliased, true)
                , COUNTER(BytesReceived, true)
                , COUNTER(SQEAllocated, true)
                , COUNTER(SubmitCount, true)
                , COUNTER(CQEProcessed, true)
                , COUNTER(EventWakeups, true)
                , COUNTER(PushedAsFirst, true)
                , COUNTER(PushedTotal, true)
                , COUNTER(ReadUnavail, true)
                , COUNTER(WriteUnavail, true)
                , COUNTER(SessionsMigratedOut, true)
                , COUNTER(SessionsMigratedIn, true)
#define TOTAL_TIME(NAME) NAME(shardCounters->GetCounter("TotalTime/" #NAME, true))
                , TOTAL_TIME(OtherTotalTime)
                , TOTAL_TIME(CompleteWaitTotalTime)
                , TOTAL_TIME(SubmitWaitTotalTime)
                , TOTAL_TIME(ApplyBytesReadTotalTime)
                , TOTAL_TIME(ApplyBytesWrittenTotalTime)
                , TOTAL_TIME(SerializeBufferTotalTime)
                , TOTAL_TIME(SerializeEventTotalTime)
                , CommandDeliveryTime(shardCounters->GetNamedHistogram("sensor", "CommandDeliveryTime", TimeCollector()))
                , EventToWireTime(shardCounters->GetNamedHistogram("sensor", "EventToWireTime", TimeCollector()))
                , CompletionWaitTime(shardCounters->GetNamedHistogram("sensor", "CompletionWaitTime", TimeCollector()))
                , CommandExecTime(shardCounters->GetNamedHistogram("sensor", "CommandExecTime", TimeCollector()))
                , SubmitExecTime(shardCounters->GetNamedHistogram("sensor", "SubmitExecTime", TimeCollector()))
                , SerializeTime(shardCounters->GetNamedHistogram("sensor", "SerializeTime", TimeCollector()))
                , CompletionsProcessedAtOnce(shardCounters->GetNamedHistogram("sensor", "CompletionsProcessedAtOnce", NMonitoring::ExponentialHistogram(10, 2)))
                , SubmissionsProcessedAtOnce(shardCounters->GetNamedHistogram("sensor", "SubmissionsProcessedAtOnce", NMonitoring::ExponentialHistogram(12, 2)))
                , Load(load)
#undef TOTAL_TIME
#undef COUNTER
            {
                EventFd = eventfd(0, 0);
                if (EventFd == -1) {
                    Y_ABORT("eventfd() failed: %s", strerror(errno));
                }

                Rings.resize(ringsPerShard);
                for (ui32 i = 0; i < Rings.size(); ++i) {
                    auto& slot = Rings[i];
                    InitRing(slot, sqpoll, sqThreadIdleMs, shareRingsWith ? &shareRingsWith->Rings[i] : nullptr);
                    if (UseEventFdAsCQ) {
                        if (int res = io_uring_register_eventfd(&slot.Ring, EventFd); res < 0) {
                            Y_ABORT("failed to register eventfd along with ring: %s", strerror(-res));
                        }
                    }
                }

                TimerFd = timerfd_create(CLOCK_MONOTONIC, 0);
                Y_ABORT_UNLESS(TimerFd != -1);

                // Keep the timer armed for the shard lifetime. Disarming while an io_uring read is
                // outstanding would leave a stuck SQE; idle CPU is controlled via sq_thread_idle instead.
                itimerspec spec{};
                spec.it_interval.tv_nsec = i64(RebalanceTimerMs) * 1000 * 1000;
                spec.it_value.tv_nsec = i64(RebalanceTimerMs) * 1000 * 1000;
                if (timerfd_settime(TimerFd, 0, &spec, nullptr) < 0) {
                    Y_ABORT("timerfd_settime failed: %s", strerror(errno));
                }
            }

            ~TShard() {
                Stop(); // joins the worker thread, so no completion will be dispatched after this point
                for (auto& slot : Rings) {
                    io_uring_queue_exit(&slot.Ring);
                }
                DrainQueue(); // free commands that were enqueued after the worker stopped (teardown races)
                close(EventFd);
                close(TimerFd);
                // remaining registered sessions are freed as the Sessions container is destroyed
            }

            void Start() {
                Worker = std::thread(std::bind(&TShard::WorkerThread, this));
            }

            ui32 PickRingIdx() {
                return NextRingIdx++ % Rings.size();
            }

            void Register(std::unique_ptr<TRegisteredSession> session) {
                ++*SessionsRegistered;
                SendInternal(reinterpret_cast<ui64>(session.release()), static_cast<ui32>(ENetwork::EvRegisterSession),
                    {}, nullptr);
            }

            void AcceptMigrated(std::unique_ptr<TRegisteredSession> session) {
                ++*SessionsMigratedIn;
                SendInternal(reinterpret_cast<ui64>(session.release()), static_cast<ui32>(ENetwork::EvRegisterSession),
                    {}, nullptr);
            }

            void Enqueue(ui64 conn, std::unique_ptr<IEventHandle> ev, TIntrusivePtr<IReceiveCallback> replyCallback) {
                SendImpl(conn, std::move(ev), std::move(replyCallback));
            }

            void Send(ui64 conn, std::unique_ptr<IEventHandle> ev, TIntrusivePtr<IReceiveCallback> replyCallback) {
                ++*EventsSent;
                SendImpl(conn, std::move(ev), std::move(replyCallback));
            }

            void Unregister(ui64 conn) {
                ++*SessionsUnregistered;
                SendInternal(conn, static_cast<ui32>(ENetwork::EvUnregisterSession), {}, nullptr);
            }

            void RegisterReceiveCallback(ui64 conn, TActorId localActorId, TIntrusivePtr<IReceiveCallback> callback) {
                ++*(callback ? DirectReceiveCallbacksRegistered : DirectReceiveCallbacksUnregistered);
                SendInternal(conn, static_cast<ui32>(ENetwork::EvRegisterCallback), localActorId, std::move(callback));
            }

            void NotifyMigrateDone(ui64 conn) {
                SendInternal(conn, static_cast<ui32>(ENetwork::EvMigrateDone), {}, nullptr);
            }

            void Stop() {
                if (Worker.joinable()) {
                    SendInternal(0, static_cast<ui32>(ENetwork::EvStop), {}, nullptr);
                    Worker.join();
                    // The worker is stopped, so it is now safe to touch the sessions directly. Shut every
                    // socket down so the peer observes the disconnect promptly instead of only when this shard
                    // (and thus the sockets it still references) is finally destroyed. The session objects
                    // themselves stay alive until the shard is destroyed.
                    for (const auto& session : Sessions) {
                        if (session->Socket) {
                            session->Socket->Shutdown(SHUT_RDWR);
                        }
                    }
                }
            }

        private:
            // Pops and frees any commands still sitting in the queue after the worker has stopped. Mirrors
            // the ownership handling of the worker loop: destroys the embedded TEventPayload and reclaims a
            // TRegisteredSession handed off via an unprocessed EvRegisterSession.
            void DrainQueue() {
                for (;;) {
                    if (auto&& [ev, conn, callback, timestamp] = IncomingEventQueue.Pop(); ev) {
                        if (ev->Type == static_cast<ui32>(ENetwork::EvRegisterSession)) {
                            delete reinterpret_cast<TRegisteredSession*>(conn);
                        }
                    } else {
                        break;
                    }
                }
            }

            void SendImpl(ui64 conn, std::unique_ptr<IEventHandle> ev, TIntrusivePtr<IReceiveCallback> replyCallback) {
                const bool first = IncomingEventQueue.Push(std::move(ev), conn, std::move(replyCallback));
                if (first) {
                    ++*PushedAsFirst;
                }
                ++*PushedTotal;
                if (first && WaitingForCQ.load()) {
                    // first command while waiting on CQ: kick the worker via the pipe on ring 0
                    const ui64 value = 1; // this commands adds 1 to the counter stored in eventfd
                    ssize_t res;
                    while ((res = write(EventFd, &value, sizeof(value))) != sizeof(value)) {
                        if (res == -1 && errno == EINTR) {
                            continue;
                        } else {
                            Y_ABORT("write() to eventfd failed: %s", strerror(errno));
                        }
                    }
                    ++*EventWakeups;
                }
            }

            void SendInternal(ui64 conn, ui32 type, TActorId sender, TIntrusivePtr<IReceiveCallback> callback) {
                SendImpl(conn, std::make_unique<IEventHandle>(type, 0, TActorId(), sender, nullptr, 0), std::move(callback));
            }

            bool ForwardIfMigratingOut(ui64 conn, std::unique_ptr<IEventHandle>& ev, TIntrusivePtr<IReceiveCallback>& callback) {
                if (const auto it = MigratingOut.find(conn); it != MigratingOut.end()) {
                    Engine->Shards.at(it->second)->Enqueue(conn, std::move(ev), std::move(callback));
                    return true;
                }
                return false;
            }

            TRingSlot& RingOf(const TRegisteredSession& session) {
                return Rings.at(session.RingIdx);
            }

            // GetSQE returns next available SQ entry, setting up ItemsToSubmit counter in order to commence submission
            // on the end of the worker loop. Event/timer control ops always use ring 0.
            io_uring_sqe *GetSQE(TRegisteredSession *session, EOperationType op) {
                TRingSlot& slot = (op == kOpEvent || op == kOpTimer || !session) ? Rings[0] : RingOf(*session);
                io_uring_sqe *sqe = io_uring_get_sqe(&slot.Ring);
                if (!sqe) { // submit queue is full: try to submit something to free it up
                    DoSubmit(slot);
                    sqe = io_uring_get_sqe(&slot.Ring);
                }
                if (sqe) {
                    ++slot.ItemsToSubmit;
                    uintptr_t sessionId = reinterpret_cast<uintptr_t>(session);
                    Y_ABORT_UNLESS((sessionId & kOpMask) == 0);
                    io_uring_sqe_set_data64(sqe, sessionId | op);
                    Y_DEBUG_ABORT_UNLESS(op == kOpEvent || op == kOpTimer ? session == nullptr : session != nullptr);
                    ++*SQEAllocated;
                }
                return sqe;
            }

            void PutEventReadRequest() {
                if (!UseEventFdAsCQ) {
                    io_uring_sqe *sqe = GetSQE(nullptr, kOpEvent);
                    Y_ABORT_UNLESS(sqe, "failed to obtain event SQE: SQ overflow");
                    io_uring_prep_read(sqe, EventFd, &EventFdReadBuffer, sizeof(EventFdReadBuffer), -1);
                }
            }

            void PutTimer() {
                io_uring_sqe *sqe = GetSQE(nullptr, kOpTimer);
                Y_ABORT_UNLESS(sqe, "failed to obtain timer SQE: SQ overflow");
                io_uring_prep_read(sqe, TimerFd, ReadTimerBuffer, sizeof(ReadTimerBuffer), -1);
            }

            // DoSubmit performs actual io_uring submit operation for all allocated entries during the worker loop
            void DoSubmit(TRingSlot& slot) {
                ui64 enterTimestamp;

                ACTIVITY(&SubmitWaitTotalTime) {
                    enterTimestamp = LastActivitySwitchTimestamp;

                    for (;;) {
                        int res = io_uring_submit(&slot.Ring);
                        if (res == -EINTR) {
                            continue;
                        }
                        if (res < 0) {
                            Y_ABORT("io_uring_submit() failed: %s", strerror(-res));
                        }
                        break;
                    }
                }

                ++*SubmitCount;
                SubmitExecTime->Collect((LastActivitySwitchTimestamp - enterTimestamp) * Freq);
                SubmissionsProcessedAtOnce->Collect(slot.ItemsToSubmit, 1u);
                slot.ItemsToSubmit = 0;
            }

            void SubmitAllPending() {
                for (auto& slot : Rings) {
                    if (slot.ItemsToSubmit) {
                        DoSubmit(slot);
                    }
                }
            }

            void WorkerThread() {
                LastActivitySwitchTimestamp = GetCycleCountFast();

                pthread_setname_np(pthread_self(), "IC_uring");

                // Arm pipe + timer on ring 0 so wait_cqe can be kicked from SendImpl / keepalive ticks.
                PutEventReadRequest();
                PutTimer();

                for (;;) {
                    const ui64 loopStartTs = GetCycleCountFast();
                    ui64 waitNs = 0;

                    // submit any pending SQ's (if we have any)
                    SubmitAllPending();

                    // wait for something to happen
                    WaitingForCQ.store(true);
                    if (IncomingEventQueue.IsEmpty()) {
                        io_uring_cqe *cqe;
                        ui64 enterTimestamp;
                        ACTIVITY(&CompleteWaitTotalTime) {
                            enterTimestamp = LastActivitySwitchTimestamp;
                            if (!UseEventFdAsCQ) {
                                if (int res = io_uring_wait_cqe(&Rings[0].Ring, &cqe); res && res != -EINTR) {
                                    Y_ABORT("io_uring_wait_cqe() failed: %s", strerror(-res));
                                }
                            } else {
                                // wait for the eventfd notification
                                ssize_t res = read(EventFd, &EventFdReadBuffer, sizeof(EventFdReadBuffer));
                                if (res == sizeof(EventFdReadBuffer)) {
                                    // TODO(alexvru): they must be 1 exactly, check the logic for excessive wakeups
                                    Y_DEBUG_ABORT_UNLESS(EventFdReadBuffer > 0, "%" PRIu64, EventFdReadBuffer);
                                } else if (res != -1 || errno != EINTR) {
                                    Y_ABORT("read() from eventfd failed: %s", strerror(errno));
                                }
                            }
                        }
                        waitNs = (LastActivitySwitchTimestamp - enterTimestamp) * Freq;
                        CompletionWaitTime->Collect(waitNs);
                    }
                    WaitingForCQ.store(false);

                    // process pending CQ events from every ring
                    i64 completionsProcessedAtOnce = 0;
                    for (auto& slot : Rings) {
                        io_uring_cqe *cqes[CqeBatchSize];
                        while (const unsigned n = io_uring_peek_batch_cqe(&slot.Ring, cqes, CqeBatchSize)) {
                            for (unsigned i = 0; i < n; ++i) {
                                DispatchCompletion(*cqes[i]);
                            }
                            io_uring_cq_advance(&slot.Ring, n);
                            completionsProcessedAtOnce += n;
                            if (n < CqeBatchSize) {
                                break;
                            }
                        }
                    }
                    if (completionsProcessedAtOnce) {
                        CompletionsProcessedAtOnce->Collect(completionsProcessedAtOnce, 1u);
                    }

                    // process pending events and commands
                    for (;;) {
                        auto&& [ev, conn, callback, cycleCountOnSend] = IncomingEventQueue.Pop();
                        if (!ev) {
                            break;
                        }

                        const ui64 cycleCountOnEnter = GetCycleCountFast();

                        switch (ev->Type) {
                            case static_cast<ui32>(ENetwork::EvRegisterCallback):
                                if (ForwardIfMigratingOut(conn, ev, callback)) {
                                    break;
                                }
                                if (TRegisteredSession& session = GetSession(conn); callback) {
                                    session.ReceiveCallbacks[ev->Sender] = std::move(callback);
                                } else {
                                    session.ReceiveCallbacks.erase(ev->Sender);
                                }
                                break;

                            case static_cast<ui32>(ENetwork::EvRegisterSession): {
                                std::unique_ptr<TRegisteredSession> session(reinterpret_cast<TRegisteredSession*>(conn));
                                const bool migrated = session->MigrateState == EMigrateState::HandedOff;
                                const ui32 sourceShard = session->MigrateSourceShard;
                                session->RingIdx = PickRingIdx();
                                session->OwnerShard.store(ShardIdx, std::memory_order_release);
                                session->MigrateState = EMigrateState::None;
                                session->MigrateSourceShard = ShardIdx;
                                const auto [it, inserted] = Sessions.emplace(std::move(session));
                                Y_ABORT_UNLESS(inserted);
                                (*it)->EventsReceived = EventsReceived;
                                IssueReadForSession(**it);
                                UpdateParkState();
                                if (migrated && sourceShard != ShardIdx) {
                                    Engine->Shards.at(sourceShard)->NotifyMigrateDone(conn);
                                }
                                break;
                            }

                            case static_cast<ui32>(ENetwork::EvUnregisterSession): {
                                if (ForwardIfMigratingOut(conn, ev, callback)) {
                                    break;
                                }
                                TRegisteredSession& session = GetSession(conn);
                                // Do NOT free the session while it still has an armed recv or an in-flight
                                // writev: their io_uring completions carry a raw pointer to this object and
                                // would dereference freed memory. Mark it terminated (so no new ops are armed)
                                // and erase only once both are drained. The session actor has already shut the
                                // socket down before requesting unregistration, so the pending ops complete
                                // promptly (EOF/EPIPE).
                                if (session.MigrateState != EMigrateState::None) {
                                    session.MigrateState = EMigrateState::None; // unregister wins over migrate
                                }
                                session.Terminated = true;
                                session.UnregisterRequested = true;
                                MaybeEraseSession(session);
                                break;
                            }

                            case static_cast<ui32>(ENetwork::EvMigrateDone):
                                MigratingOut.erase(conn);
                                break;

                            case static_cast<ui32>(ENetwork::EvStop):
                                return;

                            default: {
                                if (ForwardIfMigratingOut(conn, ev, callback)) {
                                    break;
                                }
                                TRegisteredSession& session = GetSession(conn);
                                if (callback) { // register callback coming along with the message
                                    session.ReceiveCallbacks[ev->Sender] = std::move(callback);
                                }
                                session.Serializer.Push(std::move(ev));
                                IssueWritesForSession(session);
                                break;
                            }
                        }

                        const ui64 cycleCountOnExit = GetCycleCountFast();
                        CommandDeliveryTime->Collect(NHPTimer::GetSeconds(cycleCountOnEnter - cycleCountOnSend) * 1e9);
                        CommandExecTime->Collect(NHPTimer::GetSeconds(cycleCountOnExit - cycleCountOnEnter) * 1e9);
                    }

                    const ui64 loopEndTs = GetCycleCountFast();
                    const ui64 totalNs = (loopEndTs - loopStartTs) * Freq;
                    const ui64 workNs = totalNs > waitNs ? totalNs - waitNs : 0;
                    PublishLoadSample(workNs, totalNs);
                }
            }

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // io_uring completion handlers

            void DispatchCompletion(io_uring_cqe cqe) {
                auto *session = reinterpret_cast<TRegisteredSession*>(uintptr_t(cqe.user_data) & ~uintptr_t(kOpMask));
                Y_ABORT_UNLESS(!(cqe.flags & IORING_CQE_F_MORE)); // not expecting multiple completions
                switch (static_cast<EOperationType>(cqe.user_data & kOpMask)) {
                    case kOpEvent:
                        Y_DEBUG_ABORT_UNLESS(session == nullptr);
                        Y_DEBUG_ABORT_UNLESS(cqe.res == sizeof(EventFdReadBuffer));
                        // TODO(alexvru): they must be 1 exactly, check the logic for excessive wakeups
                        Y_DEBUG_ABORT_UNLESS(EventFdReadBuffer > 0, "%" PRIu64, EventFdReadBuffer);
                        PutEventReadRequest();
                        break;

                    case kOpRead:
                        Y_DEBUG_ABORT_UNLESS(session != nullptr);
                        DispatchRead(*session, cqe.res);
                        break;

                    case kOpWrite:
                        Y_DEBUG_ABORT_UNLESS(session != nullptr);
                        DispatchWrite(*session, cqe.res);
                        break;

                    case kOpTimer:
                        Y_DEBUG_ABORT_UNLESS(session == nullptr);
                        DispatchTimer();
                        PutTimer();
                        break;

                    case kOpCancel:
                        // Original op completes with -ECANCELED; the cancel SQE itself needs no action.
                        break;
                }
                ++*CQEProcessed;
            }

            void DispatchTimer() {
                ++TimerTicks;

                if (TimerTicks % PingEveryNTicks == 0) {
                    for (auto& session : Sessions) {
                        if (!session->Terminated && session->MigrateState == EMigrateState::None && session->SendPings &&
                                session->PingRequestSentTimestamp == 0) {
                            session->SendPingRequest();
                            IssueWritesForSession(*session);
                        }
                    }

                    // Rebalance at most once per ping period to limit churn under short load spikes.
                    MaybeOffload();
                }
            }

            void MaybeOffload() {
                if (Engine->Shards.size() < 2 || Sessions.size() < 2) {
                    return;
                }
                if (Load.BusyFraction() < OffloadBusyThreshold) {
                    return;
                }

                ui32 bestTarget = ShardIdx;
                ui32 bestBusy = Max<ui32>();
                for (ui32 i = 0; i < Engine->Shards.size(); ++i) {
                    if (i == ShardIdx) {
                        continue;
                    }
                    if (const ui32 busy = Engine->ShardLoads[i].BusyFraction(); busy < bestBusy) {
                        bestBusy = busy;
                        bestTarget = i;
                    }
                }
                if (bestTarget == ShardIdx || bestBusy > StealBusyThreshold) {
                    return;
                }

                TRegisteredSession *candidate = nullptr;
                for (auto& session : Sessions) {
                    if (session->IsMigratable()) {
                        candidate = session.get();
                        break;
                    }
                }
                if (!candidate) {
                    return;
                }

                StartMigrate(*candidate, bestTarget);
            }

            void StartMigrate(TRegisteredSession& session, ui32 targetShard) {
                Y_ABORT_UNLESS(session.IsMigratable());
                session.MigrateState = EMigrateState::Draining;
                session.MigrateTargetShard = targetShard;
                session.MigrateSourceShard = ShardIdx;

                if (session.ReadPending) {
                    CancelOp(session, kOpRead);
                }
                // Write is required clear by IsMigratable(); still assert.
                Y_ABORT_UNLESS(!session.WritePending);

                MaybeFinishMigrate(session);
            }

            void CancelOp(TRegisteredSession& session, EOperationType op) {
                io_uring_sqe *sqe = GetSQE(&session, kOpCancel);
                Y_ABORT_UNLESS(sqe, "failed to obtain cancel SQE");
                const ui64 targetUserData = reinterpret_cast<uintptr_t>(&session) | op;
                io_uring_prep_cancel64(sqe, targetUserData, 0);
            }

            void MaybeFinishMigrate(TRegisteredSession& session) {
                if (session.MigrateState != EMigrateState::Draining) {
                    return;
                }
                if (session.ReadPending || session.WritePending || session.UnregisterRequested || session.Terminated) {
                    return;
                }

                const ui64 conn = reinterpret_cast<ui64>(&session);
                const ui32 target = session.MigrateTargetShard;
                session.MigrateState = EMigrateState::HandedOff;

                auto it = Sessions.find(&session);
                Y_ABORT_UNLESS(it != Sessions.end());
                // THashSet iterators yield const unique_ptr&; release ownership then erase the empty slot.
                std::unique_ptr<TRegisteredSession> owned(const_cast<std::unique_ptr<TRegisteredSession>&>(*it).release());
                Sessions.erase(it);
                UpdateParkState();

                MigratingOut[conn] = target;
                ++*SessionsMigratedOut;
                Engine->Shards.at(target)->AcceptMigrated(std::move(owned));
            }

            void DispatchRead(TRegisteredSession& session, i32 res) {
                Y_DEBUG_ABORT_UNLESS(session.ReadPending);
                session.ReadPending = false;

                if (session.MigrateState == EMigrateState::Draining) {
                    MaybeFinishMigrate(session); // NB: may free/move `session`
                    return;
                }

                if (session.Terminated) {
                    // teardown in progress: don't process further data or re-arm; just let the session drain
                    // toward erasure below
                } else if (res == -ECANCELED) {
                    // cancelled without migrate (should be rare); re-arm unless terminating
                    IssueReadForSession(session);
                } else if (res == -EAGAIN) {
                    ++*ReadUnavail;
                    IssueReadForSession(session);
                } else if (res < 0) {
                    session.Disconnect(TDisconnectReason::FromErrno(-res));
                } else if (res == 0) {
                    session.Disconnect(TDisconnectReason::EndOfStream());
                } else {
                    *BytesReceived += res;
                    Load.Bytes.fetch_add(res, std::memory_order_relaxed);
                    ACTIVITY(&ApplyBytesReadTotalTime) {
                        session.ApplyBytesRead(res);
                    }
                    IssueReadForSession(session);
                    IssueWritesForSession(session);
                }

                MaybeEraseSession(session); // NB: may free `session`; must be the last use
            }

            void IssueReadForSession(TRegisteredSession& session) {
                if (session.Terminated || session.MigrateState != EMigrateState::None) {
                    return;
                }
                Y_DEBUG_ABORT_UNLESS(!session.ReadPending);
                TMutableContiguousSpan span = session.GetReadSpan();
                io_uring_sqe *sqe = GetSQE(&session, kOpRead);
                Y_ABORT_UNLESS(sqe);
                io_uring_prep_read(sqe, *session.Socket, span.data(), span.size(), -1);
                session.ReadPending = true;
            }

            void DispatchWrite(TRegisteredSession& session, i32 res) {
                Y_ABORT_UNLESS(session.WritePending);
                session.WritePending = false;

                if (session.MigrateState == EMigrateState::Draining) {
                    MaybeFinishMigrate(session);
                    return;
                }

                if (session.Terminated) {
                    // teardown in progress: don't retry the write or re-arm; just let the session drain
                    // toward erasure below
                } else if (res == -ECANCELED) {
                    IssueWritesForSession(session);
                } else if (res == -EAGAIN) {
                    ++*WriteUnavail;
                    SubmitIovec(session);
                } else if (res < 0) {
                    session.Disconnect(TDisconnectReason::FromErrno(-res));
                } else if (res == 0) {
                    session.Disconnect(TDisconnectReason::EndOfStream());
                } else {
                    *BytesSent += res;
                    Load.Bytes.fetch_add(res, std::memory_order_relaxed);
                    ACTIVITY(&ApplyBytesWrittenTotalTime) {
                        session.ApplyBytesWritten(res, &EventToWireTimeVec);
                        for (const ui64 time : EventToWireTimeVec) {
                            EventToWireTime->Collect(time * Freq, 1u);
                        }
                        EventToWireTimeVec.clear();
                    }
                    IssueWritesForSession(session);
                }

                MaybeEraseSession(session); // NB: may free `session`; must be the last use
            }

            void IssueWritesForSession(TRegisteredSession& session) {
                if (session.WritePending
                        || session.Terminated
                        || session.MigrateState != EMigrateState::None
                        || !session.Serializer.IsTrafficPending())
                {
                    return;
                }
                if (session.Serialize()) {
                    const ui64 serializeBufferTime = session.Serializer.GetSerializeBufferTime();
                    const ui64 serializeEventTime = session.Serializer.GetSerializeEventTime();
                    const ui64 prevTimestamp = std::exchange(LastActivitySwitchTimestamp, GetCycleCountFast());
                    **CurrentActivityTime += (LastActivitySwitchTimestamp - prevTimestamp) * Freq - (serializeBufferTime + serializeEventTime);
                    *SerializeBufferTotalTime += serializeBufferTime;
                    *SerializeEventTotalTime += serializeEventTime;
                    *BytesCopied += session.Serializer.GetBytesCopied();
                    *BytesAliased += session.Serializer.GetBytesAliased();
                }
                if (session.PrepareIovec()) {
                    SubmitIovec(session);
                }
            }

            void SubmitIovec(TRegisteredSession& session) {
                if (session.MigrateState != EMigrateState::None) {
                    return;
                }
                io_uring_sqe *sqe = GetSQE(&session, kOpWrite);
                Y_ABORT_UNLESS(sqe);
                io_uring_prep_writev(sqe, *session.Socket, session.Iov, session.IovLen, -1);
                session.WritePending = true;
            }

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // commands from outer threads

            TRegisteredSession& GetSession(ui64 conn) const {
                TRegisteredSession *ptr = reinterpret_cast<TRegisteredSession*>(conn);
                Y_ABORT_UNLESS(Sessions.find(ptr) != Sessions.end());
                return *ptr;
            }

            // Frees an unregistered session once it has no io_uring operation in flight. It is unsafe to
            // erase earlier because any pending read/write completion references the session by raw pointer.
            void MaybeEraseSession(TRegisteredSession& session) {
                if (session.UnregisterRequested && !session.ReadPending && !session.WritePending) {
                    auto it = Sessions.find(&session);
                    Y_ABORT_UNLESS(it != Sessions.end());
                    Sessions.erase(it);
                    UpdateParkState();
                }
            }
        };

        std::vector<std::unique_ptr<TShard>> Shards;
        std::vector<TShardLoad> ShardLoads;
        std::atomic_uint64_t NextShardIdx;

        NMonitoring::TDynamicCounterPtr UringCounters;

    public:
        TUringEngine(ui32 numShards, NMonitoring::TDynamicCounterPtr counters, bool sqpoll, ui32 ringsPerShard,
                ui32 sqThreadIdleMs, bool shareRingsAmongThreads)
            : UringCounters(std::move(counters))
        {
            ShardLoads = std::vector<TShardLoad>(numShards);
            Shards.reserve(numShards);
            for (ui32 i = 0; i < numShards; ++i) {
                Shards.push_back(std::make_unique<TShard>(this, i,
                    UringCounters->GetSubgroup("shard", "0" /*ToString(i)*/), sqpoll, ringsPerShard, sqThreadIdleMs,
                    ShardLoads[i], shareRingsAmongThreads && !Shards.empty() ? Shards.front().get() : nullptr));
            }
            for (auto& shard : Shards) {
                shard->Start();
            }
        }

        ~TUringEngine() {
            Stop();
        }

        void SetActorSystem(TActorSystem* actorSystem) override {
            Y_ABORT_UNLESS(actorSystem);
            ActorSystem = actorSystem;
            // Stop the reaper threads while the actor system is still up, so no completion is posted to a
            // torn-down system.
            actorSystem->DeferPreStop([self = TIntrusivePtr<IUringEngine>(this)] { self->Stop(); });
        }

        ui64 Register(TIntrusivePtr<NInterconnect::TStreamSocket> socket, const TActorId& sessionActorId,
                bool checksumming, TScopeId peerScopeId, std::function<void(TDisconnectReason)> onDisconnectCallback,
                bool sendPings, std::shared_ptr<std::atomic<int64_t>> clockSkew,
                std::shared_ptr<std::atomic<uint64_t>> pingRTT) override {
            if (Stopping) {
                return 0; // engine is shutting down; caller treats 0 as a failed registration and terminates
            }
            Y_ABORT_UNLESS(ActorSystem);

            // Prefer the currently least-loaded shard (in-process signal); fall back to round-robin.
            ui32 shardIdx = NextShardIdx++ % Shards.size();
            ui32 bestBusy = ShardLoads[shardIdx].BusyFraction();
            for (ui32 i = 0; i < ShardLoads.size(); ++i) {
                if (const ui32 busy = ShardLoads[i].BusyFraction(); busy < bestBusy) {
                    bestBusy = busy;
                    shardIdx = i;
                }
            }

            // RingIdx is assigned on the shard worker when the session is inserted (supports migrate re-pin).
            auto session = std::make_unique<TRegisteredSession>(shardIdx, /*ringIdx=*/0, std::move(socket), sessionActorId,
                checksumming, peerScopeId, std::move(onDisconnectCallback), ActorSystem, sendPings, std::move(clockSkew),
                std::move(pingRTT));
            const ui64 conn = reinterpret_cast<ui64>(session.get());
            Shards[shardIdx]->Register(std::move(session));
            return conn;
        }

        TShard& GetShard(ui64 conn) const {
            // OwnerShard is the indirection for dynamic mapping: stable conn handle, mutable owner.
            return *Shards.at(reinterpret_cast<TRegisteredSession*>(conn)->OwnerShard.load(std::memory_order_acquire));
        }

        void Send(ui64 conn, std::unique_ptr<IEventHandle> ev, TIntrusivePtr<IReceiveCallback> replyCallback) override {
            // Sessions may still forward events during actor-system teardown (DeferPreStop runs Stop() before
            // executor threads are joined), so drop rather than abort. Checking Stopping before touching the
            // shard/conn keeps this safe: Stop() only joins workers, it never frees shards or sessions.
            if (Stopping) {
                return;
            }
            GetShard(conn).Send(conn, std::move(ev), std::move(replyCallback));
        }

        void Unregister(ui64 conn) override {
            if (Stopping) {
                return;
            }
            GetShard(conn).Unregister(conn);
        }

        void RegisterReceiveCallback(ui64 conn, TActorId localActorId, TIntrusivePtr<IReceiveCallback> callback) override {
            if (Stopping) {
                return;
            }
            GetShard(conn).RegisterReceiveCallback(conn, localActorId, std::move(callback));
        }

        void Stop() override {
            // Quiesce the reaper/worker threads (so no completion is posted to a torn-down actor system) but
            // keep shards and their registered sessions alive: executor threads may still be running and may
            // call in with live conn pointers. The memory is released later in the destructor, once the actor
            // system is fully stopped and no more calls can arrive.
            if (!Stopping.exchange(true)) {
                for (auto& shard : Shards) {
                    shard->Stop();
                }
            }
        }
    };

    TUringEnginePtr CreateUringEngine(ui32 numShards, NMonitoring::TDynamicCounterPtr counters, bool sqpoll,
            ui32 ringsPerShard, ui32 sqThreadIdleMs, bool shareRingsAmongThreads) {
        if (!TUringContext::IsAvailable()) {
            return nullptr;
        }
        if (numShards < 1) {
            numShards = 1;
        }
        if (ringsPerShard < 1) {
            ringsPerShard = 1;
        }
        if (sqThreadIdleMs < 1) {
            sqThreadIdleMs = TUringContext::SqThreadIdleMs;
        }
        return MakeIntrusive<TUringEngine>(numShards, std::move(counters), sqpoll, ringsPerShard, sqThreadIdleMs,
            shareRingsAmongThreads);
    }

} // namespace NActors
