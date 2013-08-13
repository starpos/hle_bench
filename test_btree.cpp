#include <cstdio>
#include <iostream>
#include <map>
#include "random.hpp"
#include "btree.hpp"

int cmp(const void *keyPtr0, UNUSED uint16_t keySize0,
        const void *keyPtr1, UNUSED uint16_t keySize1)
{
    uint32_t a, b;
    assert(keySize0 == sizeof(uint32_t));
    assert(keySize1 == sizeof(uint32_t));
    a = *reinterpret_cast<const uint32_t *>(keyPtr0);
    b = *reinterpret_cast<const uint32_t *>(keyPtr1);
    if (a < b) return -1;
    if (a == b) return 0;
    return 1;
}

void testPage0()
{
    ::printf("%zu\n", sizeof(cybozu::Page));
    cybozu::Page page(cmp);
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

    cybozu::Page::Iterator it = page.begin();
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
    cybozu::Page page0(cmp);

    for (int i = 0; i < 10; i++) {
        uint32_t r = rand();
        page0.insert(r, r);
    }
    page0.print<uint32_t, uint32_t>();
    cybozu::Page page1 = page0;

    auto p = page0.split();
    cybozu::Page &p0 = *p.first;
    cybozu::Page &p1 = *p.second;
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
            ::printf("delete %u %u\n", it0.key(), it1->first);
            it0.erase();
            ::printf("delete done\n");
            if (!m0.isValid()) {
                m0.print();
                ::exit(1);
            }
            m1.erase(it1);
        } else {
            if (it0.isEnd() != (it1 == m1.end())) {
                ::printf("search %u\n", r);
                if (!it0.isEnd()) {
                    ::printf("btreemap: %u\n", it0.key());
                } else {
                    ::printf("std::map: %u\n", it1->first);
                }
                m0.print();
                ::exit(1);
            }
        }
        checkEquality(m0, m1);

        /* Insertion */
        r = rand();
        UNUSED bool ret0, ret1;
        ::printf("try to insert %u\n", r);
        ret0 = m0.insert(r, r);
        ::printf("insert done\n");
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

void testBtreeMap1()
{
    cybozu::BtreeMap<uint32_t, uint32_t> m;

    cybozu::util::Random<uint32_t> rand(0, 10000);
    for (size_t i = 0; i < 50; i++) {
        //uint32_t r = rand();
        uint32_t r = i;
        ::printf("try to insert %u %u\n", r, r);
        m.insert(r, r);
        //m.print();
    }
    m.print();

#if 0
    {
        cybozu::BtreeMap<uint32_t, uint32_t>::PageIterator it
            = m.beginPage();
        while (it != m.endPage()) {
            ::printf("%p\n", it.page());
            ++it;
        }
    }
    ::printf("\n");
    {
        cybozu::BtreeMap<uint32_t, uint32_t>::PageIterator it
            = m.endPage();
        while (it != m.beginPage()) {
            --it;
            ::printf("%p\n", it.page());
        }
    }
    ::printf("\n");
    {
        cybozu::BtreeMap<uint32_t, uint32_t>::ItemIterator it
            = m.beginItem();
        while (it != m.endItem()) {
            assert(!it.isEnd());
            ::printf("%u %u\n", it.key(), it.value());
            ++it;
        }
    }
    ::printf("\n");
    {
        cybozu::BtreeMap<uint32_t, uint32_t>::ItemIterator it
            = m.endItem();
        while (it != m.beginItem()) {
            --it;
            assert(!it.isEnd());
            ::printf("%u %u\n", it.key(), it.value());
            //it.print();
            //m.beginItem().print();
            //::printf("%d\n", it != m.beginItem());
        }
    }
#endif
#if 0
    {
        auto it = m.lowerBound(0);
        it.erase();

        m.print();
    }
#endif
#if 1
    {
        /* Erase test. */
        cybozu::BtreeMap<uint32_t, uint32_t>::ItemIterator it
            = m.beginItem();
        while (it != m.endItem()) {
            if (it.key() % 2 == 0) {
                ::printf("delete %u\n", it.key());
                it.erase();
            } else {
                ++it;
            }
        }
    }
    {
        cybozu::BtreeMap<uint32_t, uint32_t>::ItemIterator it
            = m.beginItem();
        it.erase();
        it.erase();
        it.erase();
        it.erase();
        it.erase();
        it.erase();
        m.print();
    }
#endif


    ::printf("------------------------\n");
    m.clear();
    for (uint32_t i = 50; 0 < i; i--) {
        ::printf("insert %u\n", i - 1);
        m.insert(i - 1, i - 1);
    }


#if 1

    ::printf("------------------------\n");
    for (size_t i = 0; i < 100; i++) {
        uint32_t r = rand();
        ::printf("try to insert %u\n", r);
        m.insert(r, r);
        //m.print(); /* debug */
    }
#endif

#if 1
    for (size_t i = 0; i < 10000; i++) {
        auto it = m.lowerBound(rand());
        if (!it.isEnd()) {
            ::printf("delete %u\n", it.key());
            it.erase();
            if (!m.isValid()) {
                m.print();
                ::exit(1);
            }
        }

        uint32_t r = rand();
        ::printf("try to insert %u\n", r);
        m.insert(r, r);
        if (!m.isValid()) {
            m.print();
            ::exit(1);
        }
    }
#endif

}

int main()
{
#if 0
    testPage0();
    testPage1();
#endif
    testBtreeMap0();
    //testBtreeMap1();
}
