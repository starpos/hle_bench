#include <cstdio>
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

void testBtreeMap0()
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
    {
        cybozu::BtreeMap<uint32_t, uint32_t>::ConstIterator it
            = m.begin();
        while (it != m.end()) {
            ::printf("%u %u\n", it.key(), it.value());
            ++it;
        }
    }
    {
        cybozu::BtreeMap<uint32_t, uint32_t>::ConstIterator it
            = m.end();
        while (it != m.begin()) {
            --it;
            ::printf("%u %u\n", it.key(), it.value());
        }
    }
}

int main()
{
#if 0
    testPage0();
    testPage1();
#endif
    testBtreeMap0();
}
