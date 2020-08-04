#include <string>
#include <tuple>
#include <utility>

struct AnyArg {
    AnyArg() = delete;
};

template <typename T>
struct Opaque {
    Opaque() = delete;
};

template <typename T>
struct Nullable {
    Nullable(std::nullptr_t) : ptr(nullptr) {}  // NOLINT
    Nullable(const T& t) : ptr(new T(t)) {}
    ~Nullable() {
        if (ptr != nullptr) {
            delete ptr;
        }
    }
    const T& value() { return *ptr; }
    bool is_null() const { return ptr == nullptr; }
    T* ptr;
};

template <typename... T>
struct Tuple {
    Tuple(T&&... t) : tuple(std::make_tuple(std::forward<T>(t)...)){};
    std::tuple<T...> tuple;

    template <size_t I>
    auto& get() const {
        return std::get<I>(this->tuple);
    }
};

struct Timestamp {};
struct Date {};
struct StringRef {};

template <typename T>
struct ListRef {};

template <typename T>
struct CCallDataTypeTrait {
    using LiteralTag = T;
};
template <typename V>
struct CCallDataTypeTrait<V*> {
    using LiteralTag = Opaque<V>;
};
template <>
struct CCallDataTypeTrait<Timestamp*> {
    using LiteralTag = Timestamp;
};
template <>
struct CCallDataTypeTrait<Date*> {
    using LiteralTag = Date;
};
template <>
struct CCallDataTypeTrait<StringRef*> {
    using LiteralTag = StringRef;
};
template <typename V>
struct CCallDataTypeTrait<ListRef<V>*> {
    using LiteralTag = ListRef<V>;
};

template <>
struct CCallDataTypeTrait<Timestamp> {
    using LiteralTag = AnyArg;
};
template <>
struct CCallDataTypeTrait<Date> {
    using LiteralTag = AnyArg;
};
template <>
struct CCallDataTypeTrait<StringRef> {
    using LiteralTag = AnyArg;
};
template <typename V>
struct CCallDataTypeTrait<ListRef<V>> {
    using LiteralTag = AnyArg;
};

/**
 * If return by arg, check external c function args
 * match declared type signature.
 */
template <bool A, bool B>
struct ConditionAnd {
    static const bool value = false;
};
template <>
struct ConditionAnd<true, true> {
    static const bool value = true;
};

template <typename Ret, typename Args, typename CRet, typename CArgs>
struct FuncTypeCheckHelper {
    static const bool value = false;
};

template <typename Arg, typename CArg>
struct FuncArgTypeCheckHelper {
    static const bool value =
        std::is_same<Arg, typename CCallDataTypeTrait<CArg>::LiteralTag>::value;
};

template <typename Ret, typename>
struct FuncRetTypeCheckHelper {
    static const bool value = false;
};
template <typename Ret>
struct FuncRetTypeCheckHelper<Ret, std::tuple<Ret*>> {
    static const bool value = true;
};
template <typename Ret>
struct FuncRetTypeCheckHelper<Nullable<Ret>, std::tuple<Ret*, bool*>> {
    static const bool value = true;
};
template <typename Ret>
struct FuncRetTypeCheckHelper<Opaque<Ret>, std::tuple<Ret*>> {
    static const bool value = true;
};

template <typename, typename>
struct FuncTupleRetTypeCheckHelper {
    using Remain = void;
    static const bool value = false;
};

template <typename TupleHead, typename... TupleTail, typename CArgHead,
          typename... CArgTail>
struct FuncTupleRetTypeCheckHelper<std::tuple<TupleHead, TupleTail...>,
                                   std::tuple<CArgHead, CArgTail...>> {
    using HeadCheck = FuncRetTypeCheckHelper<TupleHead, std::tuple<CArgHead>>;
    using TailCheck = FuncTupleRetTypeCheckHelper<std::tuple<TupleTail...>,
                                                  std::tuple<CArgTail...>>;
    using Remain = typename TailCheck::Remain;
    static const bool value =
        ConditionAnd<HeadCheck::value, TailCheck::value>::value;
};

template <typename TupleHead, typename... TupleTail, typename CArgHead,
          typename... CArgTail>
struct FuncTupleRetTypeCheckHelper<
    std::tuple<Nullable<TupleHead>, TupleTail...>,
    std::tuple<CArgHead, bool*, CArgTail...>> {
    using HeadCheck = FuncRetTypeCheckHelper<TupleHead, std::tuple<CArgHead>>;
    using TailCheck = FuncTupleRetTypeCheckHelper<std::tuple<TupleTail...>,
                                                  std::tuple<CArgTail...>>;
    using Remain = typename TailCheck::Remain;
    static const bool value =
        ConditionAnd<HeadCheck::value, TailCheck::value>::value;
};

template <typename... TupleArgs, typename... TupleTail, typename CArgHead,
          typename... CArgTail>
struct FuncTupleRetTypeCheckHelper<
    std::tuple<Tuple<TupleArgs...>, TupleTail...>,
    std::tuple<CArgHead, CArgTail...>> {
    using RecCheck =
        FuncTupleRetTypeCheckHelper<std::tuple<TupleArgs...>,
                                    std::tuple<CArgHead, CArgTail...>>;
    using RecRemain = typename RecCheck::Remain;
    using TailCheck =
        FuncTupleRetTypeCheckHelper<std::tuple<TupleTail...>, RecRemain>;
    using Remain = typename TailCheck::Remain;
    static const bool value =
        ConditionAnd<RecCheck::value, TailCheck::value>::value;
};

template <typename... CArgs>
struct FuncTupleRetTypeCheckHelper<std::tuple<>, std::tuple<CArgs...>> {
    static const bool value = true;
    using Remain = std::tuple<CArgs...>;
};

template <typename... TupleArgs, typename... CArgs>
struct FuncRetTypeCheckHelper<Tuple<TupleArgs...>, std::tuple<CArgs...>> {
    using RecCheck = FuncTupleRetTypeCheckHelper<std::tuple<TupleArgs...>,
                                                 std::tuple<CArgs...>>;
    static const bool value =
        ConditionAnd<RecCheck::value, std::is_same<typename RecCheck::Remain,
                                                   std::tuple<>>::value>::value;
};

template <typename Ret, typename ArgHead, typename... ArgTail, typename CRet,
          typename CArgHead, typename... CArgTail>
struct FuncTypeCheckHelper<Ret, std::tuple<ArgHead, ArgTail...>, CRet,
                           std::tuple<CArgHead, CArgTail...>> {
    using HeadCheck = FuncArgTypeCheckHelper<ArgHead, CArgHead>;

    using TailCheck = FuncTypeCheckHelper<Ret, std::tuple<ArgTail...>, CRet,
                                          std::tuple<CArgTail...>>;

    static const bool value =
        ConditionAnd<HeadCheck::value, TailCheck::value>::value;
};

template <typename Ret, typename ArgHead, typename... ArgTail, typename CRet,
          typename CArgHead, typename... CArgTail>
struct FuncTypeCheckHelper<Ret, std::tuple<Nullable<ArgHead>, ArgTail...>, CRet,
                           std::tuple<CArgHead, bool, CArgTail...>> {
    using HeadCheck = FuncArgTypeCheckHelper<ArgHead, CArgHead>;

    using TailCheck = FuncTypeCheckHelper<Ret, std::tuple<ArgTail...>, CRet,
                                          std::tuple<CArgTail...>>;

    static const bool value =
        ConditionAnd<HeadCheck::value, TailCheck::value>::value;
};

template <typename, typename>
struct FuncTupleArgTypeCheckHelper {
    using Remain = void;
    static const bool value = false;
};

template <typename TupleHead, typename... TupleTail, typename CArgHead,
          typename... CArgTail>
struct FuncTupleArgTypeCheckHelper<std::tuple<TupleHead, TupleTail...>,
                                   std::tuple<CArgHead, CArgTail...>> {
    using HeadCheck = FuncArgTypeCheckHelper<TupleHead, CArgHead>;
    using TailCheck = FuncTupleArgTypeCheckHelper<std::tuple<TupleTail...>,
                                                  std::tuple<CArgTail...>>;
    using Remain = typename TailCheck::Remain;
    static const bool value =
        ConditionAnd<HeadCheck::value, TailCheck::value>::value;
};

template <typename TupleHead, typename... TupleTail, typename CArgHead,
          typename... CArgTail>
struct FuncTupleArgTypeCheckHelper<
    std::tuple<Nullable<TupleHead>, TupleTail...>,
    std::tuple<CArgHead, bool, CArgTail...>> {
    using HeadCheck = FuncArgTypeCheckHelper<TupleHead, CArgHead>;
    using TailCheck = FuncTupleArgTypeCheckHelper<std::tuple<TupleTail...>,
                                                  std::tuple<CArgTail...>>;
    using Remain = typename TailCheck::Remain;
    static const bool value =
        ConditionAnd<HeadCheck::value, TailCheck::value>::value;
};

template <typename... TupleArgs, typename... TupleTail, typename CArgHead,
          typename... CArgTail>
struct FuncTupleArgTypeCheckHelper<
    std::tuple<Tuple<TupleArgs...>, TupleTail...>,
    std::tuple<CArgHead, CArgTail...>> {
    using RecCheck =
        FuncTupleArgTypeCheckHelper<std::tuple<TupleArgs...>,
                                    std::tuple<CArgHead, CArgTail...>>;
    using RecRemain = typename RecCheck::Remain;
    using TailCheck =
        FuncTupleArgTypeCheckHelper<std::tuple<TupleTail...>, RecRemain>;
    using Remain = typename TailCheck::Remain;
    static const bool value =
        ConditionAnd<RecCheck::value, TailCheck::value>::value;
};

template <typename... CArgs>
struct FuncTupleArgTypeCheckHelper<std::tuple<>, std::tuple<CArgs...>> {
    static const bool value = true;
    using Remain = std::tuple<CArgs...>;
};

template <typename Ret, typename... TupleArgs, typename... ArgTail,
          typename CRet, typename CArgHead, typename... CArgTail>
struct FuncTypeCheckHelper<Ret, std::tuple<Tuple<TupleArgs...>, ArgTail...>,
                           CRet, std::tuple<CArgHead, CArgTail...>> {
    using HeadCheck =
        FuncTupleArgTypeCheckHelper<std::tuple<TupleArgs...>,
                                    std::tuple<CArgHead, CArgTail...>>;

    using TailCheck = FuncTypeCheckHelper<Ret, std::tuple<ArgTail...>, CRet,
                                          typename HeadCheck::Remain>;

    static const bool value =
        ConditionAnd<HeadCheck::value, TailCheck::value>::value;
};

// All input arg check passed, check return by arg convention
template <typename Ret, typename CArgHead, typename... CArgTail>
struct FuncTypeCheckHelper<Ret, std::tuple<>, void,
                           std::tuple<CArgHead, CArgTail...>> {
    static const bool value =
        FuncRetTypeCheckHelper<Ret, std::tuple<CArgHead, CArgTail...>>::value;
};

// All input arg check passed, check simple return
template <typename Ret, typename CRet>
struct FuncTypeCheckHelper<Ret, std::tuple<>, CRet, std::tuple<>> {
    static const bool value = FuncArgTypeCheckHelper<Ret, CRet>::value;
};

template <typename A, typename B, typename C, typename D>
void check() {
    using Check = FuncTypeCheckHelper<A, B, C, D>;
    static_assert(Check::value, "error");
}

template <typename A, typename B, typename C, typename D>
void check_fail() {
    using Check = FuncTypeCheckHelper<A, B, C, D>;
    static_assert(!Check::value, "error");
}

int main() {
    // normal arg
    check<int, std::tuple<int, int>, int, std::tuple<int, int>>();

    // c arg not enough
    check_fail<int, std::tuple<int, int>, int, std::tuple<int>>();

    // c arg is too much
    check_fail<int, std::tuple<int>, int, std::tuple<int, int>>();

    // return by arg
    check<int, std::tuple<int>, void, std::tuple<int, int*>>();
    check_fail<int, std::tuple<int>, void, std::tuple<int>>();

    // struct arg
    check<int, std::tuple<int, StringRef>, int, std::tuple<int, StringRef*>>();
    check_fail<int, std::tuple<int, StringRef>, int,
               std::tuple<int, StringRef>>();

    // struct return
    check<Date, std::tuple<Date, Date>, void,
          std::tuple<Date*, Date*, Date*>>();
    check_fail<Date, std::tuple<Date, Date>, void,
               std::tuple<Date*, Date*, Date>>();

    // nullable arg
    check<int, std::tuple<int, Nullable<int>, int>, int,
          std::tuple<int, int, bool, int>>();
    check_fail<int, std::tuple<int, Nullable<int>, int>, int,
               std::tuple<int, int, int>>();
    check<int, std::tuple<int, Nullable<StringRef>, int>, int,
          std::tuple<int, StringRef*, bool, int>>();
    check_fail<int, std::tuple<int, Nullable<StringRef>, int>, int,
               std::tuple<int, StringRef, bool, int>>();

    // nullable return
    check<Nullable<int>, std::tuple<>, void, std::tuple<int*, bool*>>();
    check_fail<Nullable<int>, std::tuple<>, void, std::tuple<int*>>();

    // nullable arg and return
    check<Nullable<Date>, std::tuple<Nullable<int>>, void,
          std::tuple<int, bool, Date*, bool*>>();

    // tuple arg
    check<int, std::tuple<Tuple<Nullable<int>, int>>, int,
          std::tuple<int, bool, int>>();
    check_fail<int, std::tuple<Tuple<>>, int, std::tuple<>>();

    // nested tuple arg
    check<int, std::tuple<Tuple<float, Tuple<float, Nullable<int>>, int>>, int,
          std::tuple<float, float, int, bool, int>>();

    // tuple return
    check<Tuple<double, Nullable<int>, Nullable<StringRef>>, std::tuple<>, void,
          std::tuple<double*, int*, bool*, StringRef*, bool*>>();

    // nest tuple return
    check<Tuple<Tuple<Date, int>, Tuple<Nullable<float>>>,
          std::tuple<Tuple<int, Nullable<Date>>>, void,
          std::tuple<int, Date*, bool, Date*, int*, float*, bool*>>();

    // opaque
    check<Opaque<std::string>, std::tuple<Opaque<std::string>, int>,
          std::string*, std::tuple<std::string*, int>>();

    return 0;
}
