#include <cstdio>
#include <iostream>
#include <map>
#include "random.hpp"
#include "btree.hpp"
#include "time.hpp"

template <typename IntT>
struct CompareInt
{
    bool operator()(const void *keyPtr0, UNUSED uint16_t keySize0,
                    const void *keyPtr1, UNUSED uint16_t keySize1) const {
        const IntT &i0 = *reinterpret_cast<const IntT *>(keyPtr0);
        const IntT &i1 = *reinterpret_cast<const IntT *>(keyPtr1);
        if (i0 == i1) return 0;
        if (i0 < i1) return -1;
        return 1;
    }
};

template <typename IntT>
using Page = cybozu::PageX<CompareInt<IntT> >;

void testPage0()
{
    ::printf("%zu\n", sizeof(Page<uint32_t>));
    Page<uint32_t> page;
    //page.print();
    page.print<uint32_t, uint32_t>();

    cybozu::util::Random<uint32_t> rand(0, 255);

    ::printf("numRecords: %zu\n", page.numRecords());
    for (size_t i = 0; i < 100; i++) {
        uint32_t r = rand();
        //::printf("try to insert %0x\n", r);
        cybozu::BtreeError err;
        if (!page.insert(r, r, &err)) {
            ::printf("insertion error.\n");
        } else {
            //::printf("inserted %0x\n", r);
        }
        //page.print();
    }
    //page.print();
    ::printf("numRecords: %zu\n", page.numRecords());
    page.print<uint32_t, uint32_t>();

    Page<uint32_t>::Iterator it = page.begin();
    while (it != page.end()) {
        if (it.key<uint32_t>() % 2 == 0) {
            it = page.erase(it);
        } else {
            ++it;
        }
    }
    ::printf("numRecords: %zu\n", page.numRecords());
    //page.print();
    page.print<uint32_t, uint32_t>();

    page.gc();

    ::printf("numRecords: %zu\n", page.numRecords());
    //page.print();
    page.print<uint32_t, uint32_t>();
}

void testPage1()
{
    cybozu::util::Random<uint32_t> rand(0, 255);
    Page<uint32_t> page0;

    for (int i = 0; i < 10; i++) {
        uint32_t r = rand();
        page0.insert(r, r);
    }
    page0.print<uint32_t, uint32_t>();
    Page<uint32_t> page1 = page0;

    auto p = page0.split();
    Page<uint32_t> &p0 = *p.first;
    Page<uint32_t> &p1 = *p.second;
    p0.print<uint32_t, uint32_t>();
    p1.print<uint32_t, uint32_t>();
    UNUSED bool ret = p1.merge(p0);
    assert(ret);
    p1.print<uint32_t, uint32_t>();

    assert(page1.numRecords() == p1.numRecords());
    auto it0 = p1.cBegin();
    auto it1 = page1.cBegin();

    while (it0 != p1.cEnd() && it1 != page1.cEnd()) {
        assert(it0.key<uint32_t>() == it1.key<uint32_t>());
        assert(it0.value<uint32_t>() == it1.value<uint32_t>());
        ++it0;
        ++it1;
    }
}

template <typename Key, typename T>
void checkEquality(cybozu::BtreeMap<Key, T> &m0, std::map<Key, T> &m1)
{
    //::printf("checkEquality begin\n");
    if (m0.size() != m1.size()) {
        //::printf("size different: %zu %zu\n", m0.size(), m1.size());
        m0.print();
        ::exit(1);
    }
    //::printf("checkEquality mid0\n");
    auto it0 = m0.beginItem();
    auto it1 = m1.begin();
    while (it0 != m0.endItem() && it1 != m1.end()) {
        if (it0.key() != it1->first) {
            std::cout << "key different: "
                      << it0.key() << " " << it1->first << std::endl;
            m0.print();
            ::exit(1);
        }
        if (it0.value() != it1->second) {
            std::cout << "value different: "
                      << it0.value() << " " << it1->second << std::endl;
            m0.print();
            ::exit(1);
        }
        ++it0;
        ++it1;
    }
    //::printf("checkEquality end\n");
}

void testBtreeMap0()
{
    cybozu::BtreeMap<uint32_t, uint32_t> m0;
    std::map<uint32_t, uint32_t> m1;
    cybozu::util::Random<uint32_t> rand(0, 10000);

    /* Asc order insertion. */
    for (size_t i = 0; i < 100; i++) {
        m0.insert(i, i);
        m1.insert(std::make_pair(i, i));

        if (!m0.isValid()) {
            m0.print();
            ::exit(1);
        }
    }
    checkEquality(m0, m1);
    /* Asc order deletion. */
    for (size_t i = 0; i < 100; i++) {
        UNUSED bool ret0, ret1;
        ret0 = m0.erase(i); assert(ret0);
        ret1 = (m1.erase(i) == 1); assert(ret1);
    }
    if (!m0.empty()) {
        m0.print();
        ::exit(1);
    }
    assert(m0.empty());
    assert(m1.empty());
    m0.clear();
    m1.clear();

    /* Desc order */
    for (size_t i = 1000; 0 < i; i--) {
        m0.insert(i - 1, i - 1);
        m1.insert(std::make_pair(i - 1, i - 1));
    }
    checkEquality(m0, m1);
    for (size_t i = 1000; 0 < i; i--) {
        UNUSED bool ret0, ret1;
        ret0 = m0.erase(i - 1); assert(ret0);
        ret1 = (m1.erase(i - 1) == 1); assert(ret1);
    }
    assert(m0.empty());
    assert(m1.empty());
    m0.clear();
    m1.clear();

    /* Random */
    for (size_t i = 0; i < 1000; i++) {
        uint32_t r = rand();
        UNUSED bool ret0, ret1;
        ret0 = m0.insert(r, r);
        auto pair = m1.insert(std::make_pair(r, r));
        ret1 = pair.second;
        assert(ret0 == ret1);
    }
    checkEquality(m0, m1);

    /* Random insertion/deletion */
    for (size_t i = 0; i < 10000; i++) {
        if (i % 100 == 0) {
            ::printf("loop %zu\n", i);
        }
        /* Deletion */
        uint32_t r = rand();
        auto it0 = m0.lowerBound(r);
        auto it1 = m1.lower_bound(r);
        if (!it0.isEnd() && it1 != m1.end()) {
            //::printf("delete %u %u\n", it0.key(), it1->first);
            it0.erase();
            //::printf("delete done\n");
            if (!m0.isValid()) {
                m0.print();
                ::exit(1);
            }
            m1.erase(it1);
        } else {
            if (it0.isEnd() != (it1 == m1.end())) {
#if 0
                ::printf("search %u\n", r);
                if (!it0.isEnd()) {
                    ::printf("btreemap: %u\n", it0.key());
                } else {
                    ::printf("std::map: %u\n", it1->first);
                }
#endif
                m0.print();
                ::exit(1);
            }
        }
        checkEquality(m0, m1);

        /* Insertion */
        r = rand();
        UNUSED bool ret0, ret1;
        //::printf("try to insert %u\n", r);
        ret0 = m0.insert(r, r);
        //::printf("insert done\n");
        if (!m0.isValid()) {
            m0.print();
            ::exit(1);
        }
        auto pair = m1.insert(std::make_pair(r, r));
        ret1 = pair.second;
        assert(ret0 == ret1);
        checkEquality(m0, m1);
    }
    checkEquality(m0, m1);

    /* now editing */
}

void benchStdMap(size_t n0, uint32_t seed)
{
#if 0
    cybozu::util::Random<uint32_t> rand;
#else
    cybozu::util::XorShift128 rand(seed);
#endif

    std::map<uint32_t, uint32_t> m1;
    uint32_t total = 0;
    cybozu::time::TimeStack<> ts;

    ts.clear();
    ts.pushNow();
    for (size_t i = 0; i < n0; i++) {
        uint32_t r = rand();
        //m1.insert(std::make_pair(r, r));
        m1.emplace(r, r);
    }
    ts.pushNow();
    ::printf("std::map %zu records insertion / %lu ms\n", n0, ts.elapsedInMs());

    ts.clear();
    ts.pushNow();
    auto it1 = m1.begin();
    while (it1 != m1.end()) {
        total += it1->second;
        ++it1;
    }
    ts.pushNow();
    ::printf("std::map %zu records scan / %lu ms\n", n0, ts.elapsedInMs());

    ts.clear();
    ts.pushNow();
    for (size_t i = 0; i < n0; i++) {
        auto it = m1.lower_bound(rand());
        if (it != m1.end()) total += it->second;
    }
    ts.pushNow();
    ::printf("std::map %zu records search / %lu ms\n", n0, ts.elapsedInMs());
    
    ts.clear();
    ts.pushNow();
    for (size_t i = 0; i < n0; i++) {
        auto it = m1.lower_bound(rand());
        if (it != m1.end()) {
            m1.erase(it);
        }
        uint32_t r = rand();
        m1.emplace(r, r);
    }
    ts.pushNow();
    ::printf("std::map %zu deletion,insertion / %lu ms\n", n0, ts.elapsedInMs());
}

void benchBtreeMap(size_t n0, uint32_t seed)
{
#if 0
    cybozu::util::Random<uint32_t> rand;
#else
    cybozu::util::XorShift128 rand(seed);
#endif
    
    cybozu::BtreeMap<uint32_t, uint32_t> m0;
    uint32_t total = 0;
    cybozu::time::TimeStack<> ts;

    ts.pushNow();
    for (size_t i = 0; i < n0; i++) {
        uint32_t r = rand();
        m0.insert(r, r);
    }
    ts.pushNow();
    ::printf("btreemap %zu records insertion / %lu ms\n", n0, ts.elapsedInMs());

    ts.clear();
    ts.pushNow();
    auto it0 = m0.beginItem();
    while (!it0.isEnd()) {
	total += it0.value();
        ++it0;
    }
    ts.pushNow();
    ::printf("btreemap %zu records scan / %lu ms\n", n0, ts.elapsedInMs());

    ts.clear();
    ts.pushNow();
    for (size_t i = 0; i < n0; i++) {
        auto it = m0.lowerBound(rand());
        if (!it.isEnd()) total += it.value();
    }
    ts.pushNow();
    ::printf("btreemap %zu records search / %lu ms\n", n0, ts.elapsedInMs());

    ts.clear();
    ts.pushNow();
    for (size_t i = 0; i < n0; i++) {
        auto it = m0.lowerBound(rand());
        if (!it.isEnd()) {
            //::printf("delete %u\n", it.key());
            it.erase();
        }
        uint32_t r = rand();
        //::printf("try to insert %u\n", r);
        m0.insert(r, r);
    }
    ts.pushNow();
    ::printf("btreemap %zu deletion,insertion / %lu ms\n", n0, ts.elapsedInMs());
}

int main()
{
#if 0
    testPage0();
    testPage1();
    testBtreeMap0();
#endif
#if 1
    const size_t n = 500000;
    cybozu::util::Random<uint32_t> rand0;
    uint32_t seed = rand();
    benchBtreeMap(n, seed);
    benchStdMap(n, seed);
#endif
}
