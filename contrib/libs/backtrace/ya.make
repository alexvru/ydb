# Generated by devtools/yamaker from nixpkgs 22.11.

LIBRARY()

LICENSE(BSD-3-Clause)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(2024-06-26)

ORIGINAL_SOURCE(https://github.com/ianlancetaylor/libbacktrace/archive/4ead348bb45f753121ca0bd44170ff8352d4c514.tar.gz)

ADDINCL(
    contrib/libs/backtrace
)

NO_COMPILER_WARNINGS()

NO_RUNTIME()

CFLAGS(
    -DHAVE_CONFIG_H
)

SRCS(
    atomic.c
    backtrace.c
    dwarf.c
    fileline.c
    mmap.c
    mmapio.c
    posix.c
    print.c
    simple.c
    sort.c
    state.c
)

IF (OS_DARWIN)
    SRCS(
        macho.c
    )
ELSEIF (OS_LINUX OR OS_ANDROID)
    SRCS(
        elf.c
    )
ENDIF()

END()
