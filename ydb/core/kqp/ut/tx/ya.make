UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

IF (WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    kqp_locks_tricky_ut.cpp
    kqp_locks_ut.cpp
    kqp_mvcc_ut.cpp
    kqp_sink_locks_ut.cpp
    kqp_sink_mvcc_ut.cpp
    kqp_sink_tx_ut.cpp
    kqp_snapshot_isolation_ut.cpp
    kqp_tx_ut.cpp
)

PEERDIR(
    ydb/core/kqp
    ydb/core/kqp/ut/common
    ydb/core/tx/columnshard/hooks/testing
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
