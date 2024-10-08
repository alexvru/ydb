# Generated by devtools/yamaker.

LIBRARY()

VERSION(16.0.0)

LICENSE(Apache-2.0 WITH LLVM-exception)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

PEERDIR(
    contrib/libs/clang16
    contrib/libs/clang16/include
    contrib/libs/clang16/lib
    contrib/libs/clang16/lib/AST
    contrib/libs/clang16/lib/ASTMatchers
    contrib/libs/clang16/lib/Basic
    contrib/libs/clang16/lib/Lex
    contrib/libs/clang16/lib/Serialization
    contrib/libs/clang16/lib/Tooling
    contrib/libs/clang16/tools/extra/clang-tidy/misc
    contrib/libs/clang16/tools/extra/clang-tidy/modernize
    contrib/libs/clang16/tools/extra/clang-tidy/readability
    contrib/libs/clang16/tools/extra/clang-tidy/utils
    contrib/libs/llvm16
    contrib/libs/llvm16/lib/Frontend/OpenMP
    contrib/libs/llvm16/lib/Support
)

ADDINCL(
    contrib/libs/clang16/tools/extra/clang-tidy/cppcoreguidelines
)

NO_COMPILER_WARNINGS()

NO_UTIL()

SRCS(
    AvoidConstOrRefDataMembersCheck.cpp
    AvoidDoWhileCheck.cpp
    AvoidGotoCheck.cpp
    AvoidNonConstGlobalVariablesCheck.cpp
    AvoidReferenceCoroutineParametersCheck.cpp
    CppCoreGuidelinesTidyModule.cpp
    InitVariablesCheck.cpp
    InterfacesGlobalInitCheck.cpp
    MacroUsageCheck.cpp
    NarrowingConversionsCheck.cpp
    NoMallocCheck.cpp
    OwningMemoryCheck.cpp
    PreferMemberInitializerCheck.cpp
    ProBoundsArrayToPointerDecayCheck.cpp
    ProBoundsConstantArrayIndexCheck.cpp
    ProBoundsPointerArithmeticCheck.cpp
    ProTypeConstCastCheck.cpp
    ProTypeCstyleCastCheck.cpp
    ProTypeMemberInitCheck.cpp
    ProTypeReinterpretCastCheck.cpp
    ProTypeStaticCastDowncastCheck.cpp
    ProTypeUnionAccessCheck.cpp
    ProTypeVarargCheck.cpp
    SlicingCheck.cpp
    SpecialMemberFunctionsCheck.cpp
    VirtualClassDestructorCheck.cpp
)

END()
