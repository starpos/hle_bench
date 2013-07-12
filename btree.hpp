/**
 * B-tree in memory.
 */

#ifndef B_TREE_MEMORY_HPP
#define B_TREE_MEMORY_HPP

#include <cstdlib>
#include <cassert>
#include <thread>
#include <mutex>
#include <cstring>
#include <condition_variable>

namespace cybozu {

/**
 * Page size can be up to 32KiB.
 * because uint16_t is used for offset inside a page.
 */
constexpr unsigned int PAGE_SIZE = 4096;

/**
 * Comparison function type.
 * returned value:
 *   negative: key0 is less than key1.
 *   0:        key0 equals to key1.
 *   positive: key0 is greater than key1.
 */
using Compare = int (*)(const void *keyPtr0, uint16_t keySize0, const void *keyPtr1, uint16_t keySize1);

enum class BtreeError : uint8_t
{
    KEY_EXISTS, NO_SPACE,
};

/**
 * Stub:
 *   uint16_t off: integer indicating record position inside page.
 *   uint16_t size record size in bytes.
 *   Stub order inside a page is the key order.
 */
struct stub
{
    uint16_t off;  /* offset in the page. */
    uint16_t size; /* [byte] */
} __attribute__((packed));

/**
 * Multi granularity locking.
 */
class Mgl
{
private:
    uint16_t numS_;
    uint8_t  numX_; /* 0 or 1. */
    uint8_t  numSix_; /* 0 or 1. */
    uint16_t numIs_;
    uint16_t numIx_;
public:
    Mgl() 
        : numS_(0)
        , numX_(0)
        , numSix_(0)
        , numIs_(0)
        , numIx_(0) {
    }
    uint16_t numS() const { return numS_; }
    uint16_t numX() const { return numX_; }
    uint16_t numSix() const { return numSix_; }
    uint16_t numIs() const { return numIs_; }
    uint16_t numIx() const { return numIx_; }

    bool noS() const { return numS_ == 0; }
    bool noX() const { return numX_ == 0; }
    bool noSix() const { return numSix_ == 0; }
    bool noIs() const { return numIs_ == 0; }
    bool noIx() const { return numIx_ == 0; }

    bool canS() const { return noIx() && noSix() && noX(); }
    bool canX() const { return noIs() && noIx() && noS() && noSix() && noX(); }
    bool canSix() const { return noIx() && noS() && noSix() && noX(); }
    bool canIs() const { return noX(); }
    bool canIx() const { return noS() && noSix() && noX(); }

    void reset() {
        numS_ = 0;
        numX_ = 0;
        numSix_ = 0;
        numIs_ = 0;
        numIx_ = 0;
    }

    void print() const {
        ::printf(" (S%u X%u SIX%u IS%u IX%u)"
                 , numS_, numX_, numSix_, numIs_, numIx_);
    }

    /* now editing */

    /* try to lock. */

    /* unlock */
};

class Page
{
private:
    Compare compare_;
    char *page_;
    std::mutex mutex_;
    std::condition_variable cv_;
    Mgl mgl_;

    uint16_t recEndOff_; /* record end offset in the page. */
    uint16_t stubBgnOff_; /* stub begin offset in the page. */
    
public:
    explicit Page(Compare compare) : compare_(compare), page_(allocPageStatic()) {
        init();
    }
    ~Page() noexcept {
        assert(page_);
        ::free(page_);
    }
    void init() {
        mgl_.reset();
        recEndOff_ = 0;
        stubBgnOff_ = PAGE_SIZE;
    }
    struct stub &stub(size_t i) {
        assert(i < numStub());
        struct stub *st = reinterpret_cast<struct stub *>(page_ + stubBgnOff_);
        return st[i];
    }
    const struct stub &stub(size_t i) const {
        assert(i < numStub());
        const struct stub *st = reinterpret_cast<const struct stub *>(page_ + stubBgnOff_);
        return st[i];
    }
    size_t numStub() const {
        unsigned int bytes = PAGE_SIZE - stubBgnOff_;
        assert(bytes % sizeof(struct stub) == 0);
        return bytes / sizeof(struct stub);
    }
    void *keyPtr(size_t i) {
        return reinterpret_cast<void *>(page_ + stub(i).off + sizeof(uintptr_t));
    }
    const void *keyPtr(size_t i) const {
        return reinterpret_cast<const void *>(page_ + stub(i).off + sizeof(uintptr_t));
    }
    uint16_t keySize(size_t i) const {
        return stub(i).size - sizeof(uintptr_t);
    }
    uintptr_t value(size_t i) const {
        return *reinterpret_cast<uintptr_t *>(page_ + stub(i).off);
    }
    size_t numRecords() const {
        return numStub();
    }
    bool insert(const void *keyPtr0, uint16_t keySize0, uintptr_t value0, BtreeError &err) {
        /* Key existence check. */
        {
            uint16_t i = lowerBoundStub(keyPtr0, keySize0);
            if (i < numStub() && compare_(keyPtr0, keySize0, keyPtr(i), keySize(i)) == 0) {
                err = BtreeError::KEY_EXISTS;
                return false;
            }
        }
        
        /* Check free space. */
        uint16_t freeSpace = stubBgnOff_ - recEndOff_;
        if (freeSpace < sizeof(uintptr_t) + keySize0 + sizeof(struct stub)) {
            err = BtreeError::NO_SPACE;
            return false;
        }

        /* Allocate space for new record. */
        uint16_t recOff = recEndOff_;
        recEndOff_ += sizeof(uintptr_t) + keySize0;
        stubBgnOff_ -= sizeof(struct stub);

        /* Copy record data. */
        ::memcpy(page_ + recOff, &value0, sizeof(uintptr_t));
        ::memcpy(page_ + recOff + sizeof(uintptr_t), keyPtr0, keySize0);


        /* Insertion sort of new stub. */
        /* TODO: use lowerBound() result for more efficiency. */
        uint16_t i = 1;
        while (i < numStub()) {
            int r = compare_(keyPtr0, keySize0, keyPtr(i), keySize(i));
            assert(r != 0);
            if (r < 0) break;
            stub(i - 1) = stub(i);
            ++i;
        }
        stub(i - 1).off = recOff;
        stub(i - 1).size = sizeof(uintptr_t) + keySize0;
        
        return true;
    }
    bool erase(const void *keyPtr, uint16_t keySize) {
        uint16_t i = lowerBound(keyPtr, keySize);
        /* now editing */
        return false;
    }
    template <typename T>
    bool insert(T key, uintptr_t value0, BtreeError &err) {
        return insert(&key, sizeof(key), value0, err);
    }
    template <typename T>
    T key(size_t i) const {
        assert(sizeof(T) == keySize(i));
        return *reinterpret_cast<T *>(keyPtr(i));
    }
    void print() const {
        ::printf("Page: recEndOff %u stubBgnOff %u "
                 , recEndOff_, stubBgnOff_);
        mgl_.print();
        ::printf("\n");
        for (size_t i = 0; i < numStub(); i++) {
            const unsigned char *p = reinterpret_cast<const unsigned char *>(keyPtr(i));
            size_t s = keySize(i);
            for (size_t j = 0; j < s; j++) {
                ::printf("%02x", p[j]);
            }
            ::printf("(%zu) %p\n", s, reinterpret_cast<void *>(value(i)));
        }
    }
    /**
     * lowerBound function.
     * Binary search in stubs.
     * RETURN:
     *   uint16_t(-1) if the key is larger than all keys in the page.
     *   or the page is empty.
     */
    uint16_t lowerBoundStub(const void *keyPtr0, uint16_t keySize0) const {
        if (numStub() == 0) return uint16_t(-1);
        uint16_t i0 = 0, i1 = numStub() - 1;
        if (compare_(keyPtr(i1), keySize(i1), keyPtr0, keySize0) < 0) {
            return uint16_t(-1);
        }
        if (compare_(keyPtr0, keySize0, keyPtr(i0), keySize(i0)) < 0) {
            return 0;
        }
        
        while (i0 + 1 < i1) {
            uint16_t i = (i0 + i1) / 2;
            //::printf("i0 %u i1 %u i %u\n", i0, i1, i); //debug 
            int r = compare_(keyPtr0, keySize0, keyPtr(i), keySize(i));
            if (r == 0) {
                return i;
            } else if (r < 0) {
                i1 = i;
            } else {
                i0 = i;
            }
        }
        if (compare_(keyPtr(i0), keySize(i0), keyPtr0, keySize0) < 0) {
            assert(compare_(keyPtr0, keySize0, keyPtr(i0 + 1), keySize(i0 + 1)) <= 0);
            assert(i0 + 1 == i1);
            return i1;
        } else {
            return i0;
        }
    }
    static char *allocPageStatic() {
        void *p;
        if (::posix_memalign(&p, PAGE_SIZE, PAGE_SIZE) != 0) {
            throw std::bad_alloc();
        }
#ifdef DEBUG
        ::memset(page_, 0, PAGE_SIZE);
#endif
        return reinterpret_cast<char *>(p);
    }
};

#if 0
/**
 * Map structure using B+tree.
 *
 * T1: key type. copyable or movable.
 * T2: value type. copyable or movable.
 */
template<class Key, class T,
         class Compare = std::less<Key>,
         class Allocator = std::allocator<std::pair<const Key, T> > >
class BtreeMap
{
private:
    std::unique_ptr<>
    public:
    BtreeMap()

    BtreeMap(std::initializer_list list)
};
#endif

} //namespace cybozu

#endif /* B_TREE_MEMORY_HPP */
