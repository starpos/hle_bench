/**
 * B-tree in memory.
 */

#ifndef B_TREE_MEMORY_HPP
#define B_TREE_MEMORY_HPP

#include <cstdio>
#include <cstdlib>
#include <cassert>
#include <cstring>
#include <stdexcept>
#include <thread>
#include <mutex>
#include <sstream>
#include <memory>
#include <condition_variable>
#include "util.hpp"

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
 *   uint16_t keySize key size in bytes.
 *   uint16_t valueSize value size in bytes.
 *   Stub order inside a page is the key order.
 */
struct stub
{
    uint16_t off;  /* offset in the page. */
    uint16_t keySize; /* [byte] */
    uint16_t valueSize; /* [byte] */
} PACKED;

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

/**
 * Page header data.
 */
struct header
{
    uint16_t recEndOff; /* record end offset in the page. */
    uint16_t stubBgnOff; /* stub begin offset in the page. */
    uint16_t level; /* 0 for leaf nodes. */
    uint16_t reserved0;
    void *parent; /* parent pointer. nullptr in a root node. */
} PACKED;

/**
 * Page wrapper.
 * This store sorted key-value records.
 */
class Page
{
private:
    Compare compare_;
    std::mutex mutex_;
    std::condition_variable cv_;
    Mgl mgl_;

    /* All persistent data are stored in the page. */
    char *page_;

public:
    explicit Page(Compare compare) : compare_(compare), page_(allocPageStatic()) {
        init();
    }
    virtual ~Page() noexcept {
        ::free(page_);
    }
    void init() {
        mgl_.reset();
        clear();
    }
    /**
     * Delete all records in the page.
     */
    void clear() {
        header().recEndOff = headerEndOff();
        header().stubBgnOff = PAGE_SIZE;
#if DEBUG
        /* zero-clear */
        uint16_t size = PAGE_SIZE - headerEndOff();
        ::memset(page_ + header().recEndOff, 0, size);
#endif
    }
    size_t numRecords() const {
        return numStub();
    }
    uint16_t freeSpace() const {
        return stubBgnOff() - recEndOff();
    }
    /**
     * Total data size for record and stub.
     */
    uint16_t totalDataSize() const {
        uint16_t total = 0;
        for (uint16_t i = 0; i < numStub(); i++) {
            total += keySize(i) + valueSize(i) + sizeof(struct stub);
        }
        return total;
    }
    bool canInsert(uint16_t size) const {
        return size + sizeof(struct stub) <= freeSpace();
    }
    bool insert(const void *keyPtr0, uint16_t keySize0,
                const void *valuePtr0, uint16_t valueSize0, BtreeError *err = nullptr) {
        /* Key existence check. */
        {
            uint16_t i = lowerBoundStub(keyPtr0, keySize0);
            if (i < numStub() && compare_(keyPtr0, keySize0, keyPtr(i), keySize(i)) == 0) {
                if (err) *err = BtreeError::KEY_EXISTS;
                return false;
            }
        }

        /* Check free space. */
        if (!canInsert(keySize0 + valueSize0)) {
            if (err) *err = BtreeError::NO_SPACE;
            return false;
        }

        /* Allocate space for new record. */
        uint16_t recOff = recEndOff();
        header().recEndOff += keySize0 + valueSize0;
        header().stubBgnOff -= sizeof(struct stub);

        /* Copy record data. */
        ::memcpy(page_ + recOff, keyPtr0, keySize0);
        ::memcpy(page_ + recOff + keySize0, valuePtr0, valueSize0);

        /* Insertion sort of new stub. */
        uint16_t i = 1;
        while (i < numStub()) {
            int r = compare_(keyPtr0, keySize0, keyPtr(i), keySize(i));
            assert(r != 0);
            if (r < 0) break;
            stub(i - 1) = stub(i);
            ++i;
        }
        stub(i - 1).off = recOff;
        stub(i - 1).keySize = keySize0;
        stub(i - 1).valueSize = valueSize0;

        return true;
    }
    template <typename Key, typename T>
    bool insert(Key key, T value, BtreeError *err = nullptr) {
        return insert(&key, sizeof(key), &value, sizeof(value), err);
    }
    /**
     * remove a record.
     * Stub area will be shrinked, but record area will not.
     * You must call gc() explicitly.
     */
    bool erase(const void *keyPtr, uint16_t keySize) {
        uint16_t idx = lowerBoundStub(keyPtr, keySize);
        if (idx == uint16_t(-1)) return false;
        eraseStub(idx);
        return true;
    }
    template <typename Key>
    bool erase(Key key) {
        return erase(&key, sizeof(key));
    }
    void print() const {
        ::printf("Page: headerEndOff %u recEndOff %u stubBgnOff %u "
                 , headerEndOff(), recEndOff(), stubBgnOff());
        mgl_.print();
        ::printf("\n");
        for (size_t i = 0; i < numStub(); i++) {
            const uint8_t *p0 = reinterpret_cast<const uint8_t *>(keyPtr(i));
            const uint8_t *p1 = reinterpret_cast<const uint8_t *>(valuePtr(i));
            size_t s0 = keySize(i);
            size_t s1 = valueSize(i);
            for (size_t j = 0; j < s0; j++) ::printf("%02x", p0[j]);
            ::printf("(%zu) ", s0);
            for (size_t j = 0; j < s1; j++) ::printf("%02x", p1[j]);
            ::printf("(%zu)\n", s1);
        }
    }
    template <typename Key, typename T>
    void print() const {
        ::printf("Page: headerEndOff %u recEndOff %u stubBgnOff %u "
                 , headerEndOff(), recEndOff(), stubBgnOff());
        mgl_.print();
        ::printf("\n");
        std::stringstream ss;
        for (size_t i = 0; i < numStub(); i++) {
            ss << key<Key>(i) << " " << value<T>(i) << std::endl;
        }
        ::printf("%s", ss.str().c_str());
    }
    /**
     * Collect garbage.
     */
    void gc() {
        Page p(compare_);
        for (size_t i = 0; i < numStub(); i++) {
            UNUSED bool ret;
            ret = p.insert(keyPtr(i), keySize(i), valuePtr(i), valueSize(i));
            assert(ret);
        }
        swap(p);
    }
    static char *allocPageStatic() {
        void *p;
        if (::posix_memalign(&p, PAGE_SIZE, PAGE_SIZE) != 0) {
            throw std::bad_alloc();
        }
#ifdef DEBUG
        ::memset(p, 0, PAGE_SIZE);
#endif
        return reinterpret_cast<char *>(p);
    }

    struct header &header() {
        return *reinterpret_cast<struct header *>(page_);
    }
    const struct header &header() const {
        return *reinterpret_cast<const struct header *>(page_);
    }
    const Page *parent() const { return reinterpret_cast<const Page *>(header().parent); }
    Page *parent() { return reinterpret_cast<Page *>(header().parent); }
    bool isRoot() const { return parent() == nullptr; }
    bool isBranch() const { return header().level != 0; } /* may include root. */
    bool isLeaf() const { return header().level == 0; } /* may include root. */

    /**
     * Split a page into two pages.
     * The page will be cleared.
     */
    std::pair<std::shared_ptr<Page>, std::shared_ptr<Page> > split(bool isHalfAndHalf = true) {
        auto p0 = std::make_shared<Page>(compare_);
        auto p1 = std::make_shared<Page>(compare_);
        p0->header().level = header().level;
        p1->header().level = header().level;
        if (!isHalfAndHalf) {
            swap(*p0);
            clear();
            return std::make_pair(p0, p1);
        }
        /**
         * Insert in the reverse order for efficiency.
         */
        uint16_t n = numStub();
        UNUSED bool ret;
        for (uint16_t i = n; n / 2 < i; i--) {
            uint16_t j = i - 1;
            ret = p1->insert(keyPtr(j), keySize(j), valuePtr(j), valueSize(j));
            assert(ret);
        }
        for (uint16_t i = n / 2; 0 < i; i--) {
            uint16_t j = i - 1;
            ret = p0->insert(keyPtr(j), keySize(j), valuePtr(j), valueSize(j));
            assert(ret);
        }
        clear();
        return std::make_pair(p0, p1);
    }
    /**
     * Merge a page into the page.
     *
     * For efficient merge:
     *   *this: right page.
     *   rhs:   left page.
     *
     * RETURN:
     *   true if merge succeeded. the rhs will be cleared.
     *   false if not (data will not be changed).
     */
    bool merge(Page &rhs) {
        if (rhs.totalDataSize() < freeSpace()) {
            return false; /* no enough space. */
        }
        /* Reverse order. */
        uint16_t n = rhs.numStub();
        UNUSED bool ret;
        for (uint16_t i = n; 0 < i; i--) {
            uint16_t j = i - 1;
            ret = insert(rhs.keyPtr(j), rhs.keySize(j), rhs.valuePtr(j), rhs.valueSize(j));
            assert(ret);
        }
        rhs.clear();
        return true;
    }
    /**
     * Base class of iterators.
     */
    template <typename Base, typename It>
    class IteratorBase
    {
    private:
        friend Page;
        Base *pageP_;
        uint16_t idx_;

    public:
        IteratorBase(Base *pageP, uint16_t idx)
            : pageP_(pageP), idx_(idx) {
            assert(pageP);
        }
        It &operator=(const It &rhs) {
            pageP_ = rhs.pageP_;
            idx_ = rhs.idx_;
            return *this;
        }
        bool operator==(const It &rhs) const { return idx_ == rhs.idx_; }
        bool operator!=(const It &rhs) const { return idx_ != rhs.idx_; }
        bool operator<(const It &rhs) const { return idx_ < rhs.idx_; }
        bool operator>(const It &rhs) const { return idx_ > rhs.idx_; }
        bool operator<=(const It &rhs) const { return idx_ <= rhs.idx_; }
        bool operator>=(const It &rhs) const { return idx_ >= rhs.idx_; }
        It &operator++() {
            ++idx_;
            return *reinterpret_cast<It *>(this);
        }
        It &operator--() {
            --idx_;
            return *reinterpret_cast<It *>(this);
        }
        void *keyPtr() { return pageP_->keyPtr(idx_); }
        const void *keyPtr() const { return pageP_->keyPtr(idx_); }
        uint16_t keySize() const { return pageP_->keySize(idx_); }
        void *valuePtr() { return pageP_->valuePtr(idx_); }
        const void *valuePtr() const { return pageP_->valuePtr(idx_); }
        uint16_t valueSize() const { return pageP_->valueSize(idx_); }
        template <typename Key>
        Key key() const { return pageP_->key<Key>(idx_); }
        template <typename T>
        T value() const { return pageP_->value<T>(idx_); }
    };
    class ConstIterator : public IteratorBase<const Page, ConstIterator>
    {
    public:
        ConstIterator(const Page *pageP, uint16_t idx)
            : IteratorBase<const Page, ConstIterator>(pageP, idx) {
        }
    };
    class Iterator : public IteratorBase<Page, Iterator>
    {
    public:
        Iterator(Page *pageP, uint16_t idx)
            : IteratorBase<Page, Iterator>(pageP, idx) {
        }
        /**
         * The iterator will indicates the next record.
         */
        void erase() {
            pageP_->eraseStub(idx_);
            /* Now idx_ indicates the next record. */
        }
    };

    Iterator begin() { return Iterator(this, 0); }
    ConstIterator begin() const { return ConstIterator(this, 0); }
    ConstIterator cBegin() const { return begin(); }
    Iterator end() { return Iterator(this, numStub()); }
    ConstIterator end() const { return ConstIterator(this, numStub()); }
    ConstIterator cEnd() const { return end(); }

    /**
     * RETURN:
     *   Next item.
     */
    Iterator erase(Iterator &it) {
        it.erase();
        return it;
    }
    Iterator lowerBound(const void *keyPtr0, uint16_t keySize0) {
        uint16_t i = lowerBoundStub(keyPtr0, keySize0);
        if (i == uint16_t(-1)) i = numStub();
        return Iterator(this, i);
    }
    ConstIterator lowerBound(const void *keyPtr0, uint16_t keySize0) const {
        uint16_t i = lowerBoundStub(keyPtr0, keySize0);
        if (i == uint16_t(-1)) i = numStub();
        return ConstIterator(this, i);
    }
    template <typename T>
    ConstIterator lowerBound(T key) const { return lowerBound(&key, sizeof(T)); }
    template <typename T>
    Iterator lowerBound(T key) const { return lowerBound(&key, sizeof(T)); }
private:
    uint16_t headerEndOff() const { return sizeof(struct header); }
    uint16_t recEndOff() const { return header().recEndOff; }
    uint16_t stubBgnOff() const { return header().stubBgnOff; }
    struct stub &stub(size_t i) {
        assert(i < numStub());
        struct stub *st = reinterpret_cast<struct stub *>(page_ + stubBgnOff());
        return st[i];
    }
    const struct stub &stub(size_t i) const {
        assert(i < numStub());
        const struct stub *st = reinterpret_cast<const struct stub *>(page_ + stubBgnOff());
        return st[i];
    }
    uint16_t numStub() const {
        unsigned int bytes = PAGE_SIZE - stubBgnOff();
        assert(bytes % sizeof(struct stub) == 0);
        return bytes / sizeof(struct stub);
    }
    void *keyPtr(size_t i) {
        return reinterpret_cast<void *>(page_ + stub(i).off);
    }
    const void *keyPtr(size_t i) const {
        return reinterpret_cast<const void *>(page_ + stub(i).off);
    }
    uint16_t keySize(size_t i) const { return stub(i).keySize; }
    void *valuePtr(size_t i) {
        return reinterpret_cast<void *>(page_ + stub(i).off + stub(i).keySize);
    }
    const void *valuePtr(size_t i) const {
        return reinterpret_cast<const void *>(page_ + stub(i).off + stub(i).keySize);
    }
    uint16_t valueSize(size_t i) const { return stub(i).valueSize; }
    void swap(Page &rhs) {
        char *page = page_;
        page_ = rhs.page_;
        rhs.page_ = page;
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
    void eraseStub(size_t i) {
        assert(i < numStub());
        for (uint16_t j = i; 0 < j; j--) {
            stub(j) = stub(j - 1);
        }
        header().stubBgnOff += sizeof(struct stub);
    }
    template <typename Key>
    Key key(size_t i) const {
        assert(sizeof(Key) == keySize(i));
        return *reinterpret_cast<const Key *>(keyPtr(i));
    }
    template <typename T>
    T value(size_t i) const {
        assert(sizeof(T) == valueSize(i));
        return *reinterpret_cast<const T *>(valuePtr(i));
    }
};

#if 0
/**
 * Branch node class.
 */
class BNode : public Page
{
public:
    BNode(Compare compare, uint16_t level) : Page(compare) {
        header().level = level;
    }
    /* now editing */
};

/**
 * Leaf node class.
 */
class LNode : public Page
{
public:
    explicit LNode(Compare compare) : Page(compare) {
        header().level = 0;
    }
    LNode split(bool isHalfAndHalf = true) {
        /* now editing */
    }

/* now editing */
};
#endif

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
