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
    page.print();

    cybozu::util::Random<uint32_t> rand(0, 255);

    ::printf("numStub: %zu\n", page.numStub());
    for (size_t i = 0; i < 100; i++) {
        uint32_t r = rand();
        //::printf("try to insert %0x\n", r);
        cybozu::BtreeError err;
        if (!page.insert(r, r, err)) {
            ::printf("insertion error.\n");
        } else {
            //::printf("inserted %0x\n", r);
        }
        //page.print();
    }
    ::printf("numStub: %zu\n", page.numStub());
    page.print();

    cybozu::Page::Iterator it = page.begin();
    while (it != page.end()) {
        it = page.erase(it);
    }
    ::printf("numStub: %zu\n", page.numStub());
    page.print();

    page.gc();

    ::printf("numStub: %zu\n", page.numStub());
    page.print();
}
