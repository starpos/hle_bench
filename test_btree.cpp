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

int main()
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
