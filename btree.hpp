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
//constexpr unsigned int PAGE_SIZE = 4096;
constexpr unsigned int PAGE_SIZE = 128;

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
    KEY_EXISTS, KEY_NOT_EXISTS, NO_SPACE,
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

constexpr uint16_t EMPTY = uint16_t(-1);
constexpr uint16_t LOWER = uint16_t(-2);
constexpr uint16_t UPPER = uint16_t(-3);
static bool isNormalIndex(uint16_t idx)
{
    return idx != EMPTY && idx != LOWER && idx != UPPER;
}

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
    Page(const Page &rhs) : compare_(rhs.compare_), page_(allocPageStatic()) {
        ::memcpy(page_, rhs.page_, PAGE_SIZE);
    }
    Page(Page &&rhs) : compare_(rhs.compare_), page_(rhs.page_) {
        rhs.page_ = nullptr;
    }
    Page &operator=(const Page &rhs) {
        if (!page_) { page_ = allocPageStatic(); }
        if (page_ != rhs.page_) {
            ::memcpy(page_, rhs.page_, PAGE_SIZE);
        }
        return *this;
    }
    Page &operator=(Page &&rhs) {
        swap(rhs);
        return *this;
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
    bool empty() const {
        return stubBgnOff() == PAGE_SIZE;
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
            if (isNormalIndex(i) && compare_(keyPtr0, keySize0, keyPtr(i), keySize(i)) == 0) {
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
    bool insert(const Key &key, const T &value, BtreeError *err = nullptr) {
        return insert(&key, sizeof(key), &value, sizeof(value), err);
    }
    /**
     * remove a record.
     * Stub area will be shrinked, but record area will not.
     * You must call gc() explicitly.
     */
    bool erase(const void *keyPtr, uint16_t keySize) {
        uint16_t idx = lowerBoundStub(keyPtr, keySize);
        if (!isNormalIndex(idx)) return false;
        eraseStub(idx);
        return true;
    }
    template <typename Key>
    bool erase(const Key &key) {
        return erase(&key, sizeof(key));
    }
    /**
     * Update value of a given key.
     * RETURN:
     *   true in success.
     *   false if value size is larger than the stored (TODO).
     *         if key does not exist.
     */
    bool update(const void *keyPtr0, uint16_t keySize0, const void *valuePtr0, uint16_t valueSize0, BtreeError *err = nullptr) {
        uint16_t i = lowerBoundStub(keyPtr0, keySize0);
        if (!isNormalIndex(i) || compare_(keyPtr0, keySize0, keyPtr(i), keySize(i)) != 0) {
            if (err) *err = BtreeError::KEY_NOT_EXISTS;
            return false;
        }
        if (valueSize(i) < valueSize0) {
            if (err) *err = BtreeError::NO_SPACE;
            return false;
        }
        stub(i).valueSize = valueSize0;
        ::memcpy(valuePtr(i), valuePtr0, valueSize0);
        return true;
    }
    template <typename Key, typename T>
    bool update(const Key &key, const T &value) {
        return update(&key, sizeof(key), &value, sizeof(value));
    }
    bool isLower(const void *keyPtr0, uint16_t keySize0) const {
        assert(numStub() != 0);
        uint16_t i0 = 0;
        return compare_(keyPtr0, keySize0, keyPtr(i0), keySize(i0)) < 0;
    }
    template <typename Key>
    bool isLower(const Key &key) const {
        return isLower(&key, sizeof(key));
    }
    bool isUpper(const void *keyPtr0, uint16_t keySize0) const {
        assert(numStub() != 0);
        uint16_t i1 = numStub() - 1;
        return compare_(keyPtr(i1), keySize(i1), keyPtr0, keySize0) < 0;
    }
    template <typename Key>
    bool isUpper(const Key &key) const {
        return isUpper(&key, sizeof(key));
    }
    void print() const {
        printHeader();
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
        printHeader();
        std::stringstream ss;
        for (size_t i = 0; i < numStub(); i++) {
            ss << key<Key>(i) << " " << value<T>(i) << std::endl;
        }
        ::printf("%s", ss.str().c_str());
    }
    void printHeader() const {
        ::printf("Page: %p level %u headerEndOff %u recEndOff %u stubBgnOff %u "
                 , this, level(), headerEndOff(), recEndOff(), stubBgnOff());
        mgl_.print();
        ::printf("\n");
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
    uint16_t level() const { return header().level; }

    /**
     * Split a page into two pages.
     * The page will be cleared.
     */
    std::pair<Page *, Page *> split(bool isHalfAndHalf = true) {
        Page *p0 = new Page(compare_);
        Page *p1 = new Page(compare_);
        try {
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
        } catch (...) {
            delete p0;
            delete p1;
            throw;
        }
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
        if (freeSpace() < rhs.totalDataSize()) {
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
    protected:
        friend Page;
        Base *pageP_; /* can be null. but almost member functions does not work. */
        uint16_t idx_;

    public:
        IteratorBase(Base *pageP, uint16_t idx)
            : pageP_(pageP), idx_(idx) {
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
        bool isBegin() const { return idx_ == 0; }
        bool isEnd() const { return pageP_->numStub() <= idx_; }

        void print() const {
            ::printf("Page::Iterator %p %u\n", pageP_, idx_);
        }

        void *keyPtr() { return pageP_->keyPtr(idx_); }
        const void *keyPtr() const { return pageP_->keyPtr(idx_); }
        uint16_t keySize() const { return pageP_->keySize(idx_); }
        void *valuePtr() { return pageP_->valuePtr(idx_); }
        const void *valuePtr() const { return pageP_->valuePtr(idx_); }
        uint16_t valueSize() const { return pageP_->valueSize(idx_); }
        template <typename Key>
        const Key &key() const { return pageP_->key<Key>(idx_); }
        template <typename T>
        const T &value() const { return pageP_->value<T>(idx_); }
    };
    class ConstIterator : public IteratorBase<const Page, ConstIterator>
    {
    public:
        ConstIterator(const Page *pageP, uint16_t idx)
            : IteratorBase<const Page, ConstIterator>(pageP, idx) {
        }
        const Page *page() const { return pageP_; }
    };
    class Iterator : public IteratorBase<Page, Iterator>
    {
    public:
        Iterator(Page *pageP, uint16_t idx)
            : IteratorBase<Page, Iterator>(pageP, idx) {
        }
        Page *page() { return pageP_; }
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
        if (!isNormalIndex(i)) i = numStub();
        return Iterator(this, i);
    }
    ConstIterator lowerBound(const void *keyPtr0, uint16_t keySize0) const {
        uint16_t i = lowerBoundStub(keyPtr0, keySize0);
        if (!isNormalIndex(i)) i = numStub();
        return ConstIterator(this, i);
    }
    template <typename Key>
    ConstIterator lowerBound(const Key &key) const {
        return lowerBound(&key, sizeof(Key));
    }
    template <typename Key>
    Iterator lowerBound(const Key &key) {
        return lowerBound(&key, sizeof(Key));
    }

    template <typename Key>
    const Key &minKey() const {
        assert(!empty());
        return key<Key>(0);
    }
    template <typename Key>
    const Key &maxKey() const {
        assert(!empty());
        return key<Key>(numStub() - 1);
    }

    /**
     * These functions are used for non-leaf pages.
     */
    template <typename Key>
    Page *child(const Key &key) {
        assert(!empty());
        uint16_t i = searchStub(&key, sizeof(Key));
        if (i == LOWER) return leftMostChild();
        if (i == UPPER) return rightMostChild();
        assert(isNormalIndex(i));
        return value<Page *>(i);
    }
    template <typename Key>
    const Page *child(const Key &key) const {
        assert(!empty());
        uint16_t i = searchStub(&key, sizeof(Key));
        if (i == LOWER) return leftMostChild();
        if (i == UPPER) return rightMostChild();
        assert(isNormalIndex(i));
        return value<const Page *>(i);
    }
    Page *leftMostChild() {
        assert(!empty());
        return value<Page *>(0);
    }
    const Page *leftMostChild() const {
        assert(!empty());
        return value<const Page *>(0);
    }
    Page *rightMostChild() {
        assert(!empty());
        return value<Page *>(numStub() - 1);
    }
    const Page *rightMostChild() const {
        assert(!empty());
        return value<const Page *>(numStub() - 1);
    }

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
        //::printf("i %zu numStub %u\n", i, numStub()); /* debug */
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
    /**
     * Swap page_.
     */
    void swap(Page &rhs) {
        char *page = page_;
        page_ = rhs.page_;
        rhs.page_ = page;
    }
    /**
     * lowerBound function.
     * miminum key(i) where a specified key <= key(i)
     *
     * Binary search in stubs.
     *
     * RETURN:
     *   UPPER if the key is larger than all keys in the page,
     *   EMPTY if the page is empty.
     *   stub index for other cases (0 <= i < numStub()).
     */
    uint16_t lowerBoundStub(const void *keyPtr0, uint16_t keySize0) const {
        if (empty()) return EMPTY;
        if (isUpper(keyPtr0, keySize0)) return UPPER;
        if (isLower(keyPtr0, keySize0)) return 0;

        uint16_t i0 = 0, i1 = numStub() - 1;
        while (i0 + 1 < i1) {
            uint16_t i = (i0 + i1) / 2;
            //::printf("i0 %u i1 %u i %u\n", i0, i1, i); //debug
            int r = compare_(keyPtr0, keySize0, keyPtr(i), keySize(i));
            if (r == 0) return i;
            if (r < 0) i1 = i;
            else i0 = i;
        }
        if (compare_(keyPtr(i0), keySize(i0), keyPtr0, keySize0) < 0) {
            assert(compare_(keyPtr0, keySize0, keyPtr(i0 + 1), keySize(i0 + 1)) <= 0);
            assert(i0 + 1 == i1);
            return i1;
        } else {
            return i0;
        }
    }
    /**
     * Search a stub.
     * key(i) <= a specified key < key(i + 1)
     *
     * Binary search in stubs.
     *
     * RETURN:
     *   UPPER if the key is larger than all keys in the page,
     *   LOWER if the key is less than all keys in the page,
     *   EMPTY if the page is empty.
     *   stub index for other cases (0 <= i < numStub()).
     */
    uint16_t searchStub(const void *keyPtr0, uint16_t keySize0) const {
        if (empty()) return EMPTY;
        if (isUpper(keyPtr0, keySize0)) return UPPER;
        if (isLower(keyPtr0, keySize0)) return LOWER;

        uint16_t i0 = 0, i1 = numStub() - 1;
        while (i0 + 1 < i1) {
            uint16_t i = (i0 + i1) / 2;
            int r = compare_(keyPtr0, keySize0, keyPtr(i), keySize(i));
            if (r == 0) return i;
            if (r < 0) i1 = i;
            else i0 = i;
        }
        if (compare_(keyPtr(i1), keySize(i1), keyPtr0, keySize0) == 0) {
            return i1;
        } else {
            assert(compare_(keyPtr(i0), keySize(i0), keyPtr0, keySize0) <= 0);
            assert(compare_(keyPtr0, keySize0, keyPtr(i0 + 1), keySize(i0 + 1)) < 0);
            return i0;
        }
    }
    /**
     * Erase a stub.
     */
    void eraseStub(size_t i) {
        assert(i < numStub());
        for (uint16_t j = i; 0 < j; j--) {
            stub(j) = stub(j - 1);
        }
        header().stubBgnOff += sizeof(struct stub);
    }
    template <typename Key>
    const Key &key(size_t i) const {
        assert(sizeof(Key) == keySize(i));
        return *reinterpret_cast<const Key *>(keyPtr(i));
    }
    template <typename T>
    const T &value(size_t i) const {
        assert(sizeof(T) == valueSize(i));
        return *reinterpret_cast<const T *>(valuePtr(i));
    }
};

/**
 * A wrapper of Page class for fixed key and value type.
 */
template <typename Key, typename T,
          typename CompareT = std::less<Key> >
class PageX : public Page
{
private:
    static CompareT less_;
    static int compare_(const void *keyPtr0, uint16_t keySize0, const void *keyPtr1, uint16_t keySize1) {
        assert(sizeof(Key) == keySize0);
        assert(sizeof(Key) == keySize1);
        Key key0 = *reinterpret_cast<const Key *>(keyPtr0);
        Key key1 = *reinterpret_cast<const Key *>(keyPtr1);
        if (key0 == key1) return 0;
        if (less_(key0, key1)) return -1;
        return 1;
    }

public:
    PageX() : Page(compare_) {}
    ~PageX() noexcept = default;
    bool insert(const Key &key, const T &value, BtreeError *err = nullptr) {
        return Page::insert<Key, T>(key, value, err);
    }
    bool update(const Key &key, const T &value, BtreeError *err = nullptr) {
        return Page::update<Key, T>(key, value, err);
    }
    class Iterator : public Page::Iterator
    {
    public:
        Iterator(PageX *page, uint16_t idx)
            : Page::Iterator(page, idx) {
        }
        Key key() const { return Page::key<Key>(); }
        T value() const { return Page::value<T>(); }
    };
    class ConstIterator : public Page::ConstIterator
    {
    public:
        ConstIterator(const PageX *page, uint16_t idx)
            : Page::ConstIterator(page, idx) {
        }
        Key key() const { return Page::key<Key>(); }
        T value() const { return Page::value<T>(); }
    };

    Iterator begin() { return Iterator(this, 0); }
    Iterator end() { return Iterator(this, numStub()); }
    ConstIterator begin() const { return ConstIterator(this, 0); }
    ConstIterator end() const { return ConstIterator(this, numStub()); }
    Iterator lowerBound(const Key &key) {
        uint16_t idx = lowerBoundStub(&key, sizeof(key));
        if (!isNormalIndex(idx)) return end();
        return Iterator(this, idx);
    }
    ConstIterator lowerBound(const Key &key) const {
        uint16_t idx = lowerBoundStub(&key, sizeof(key));
        if (!isNormalIndex(idx)) return end();
        return ConstIterator(this, idx);
    }

    Key minKey() const { return Page::minKey<Key>(); }
    Key maxKey() const { return Page::maxKey<Key>(); }

    /* now editing */
};

/**
 * Map structure using B+tree.
 *
 * T1: key type. copyable or movable.
 * T2: value type. copyable or movable.
 */
template <typename Key, typename T,
          class CompareT = std::less<Key> >
class BtreeMap
{
private:
    CompareT less_;
    Page root_;

    static int compare(const void *keyPtr0, uint16_t keySize0, const void *keyPtr1, uint16_t keySize1) {
        CompareT less;
        assert(sizeof(Key) == keySize0);
        assert(sizeof(Key) == keySize1);
        const Key &key0 = *reinterpret_cast<const Key *>(keyPtr0);
        const Key &key1 = *reinterpret_cast<const Key *>(keyPtr1);
        if (key0 == key1) return 0;
        if (less(key0, key1)) return -1;
        return 1;
    }

public:
    /* now editing */
    BtreeMap() : root_(compare) {
        root_.header().level = 0;
        root_.header().parent = nullptr;
    }
    ~BtreeMap() noexcept {
        /* TODO: remove all pages except for root. */



        /* now editing */
    }
    bool insert(const Key &key, const T &value, BtreeError *err = nullptr) {
        UNUSED size_t size = sizeof(key) + sizeof(value);
        assert(size < (2 << 16));

        /* Get the corresponding leaf page. */
        Page *p = searchLeaf(key);
        assert(p->isLeaf());

        if (!p->canInsert(sizeof(key) + sizeof(value))) {
            p = splitLeaf(p, key);
        }
        assert(p->canInsert(sizeof(key) + sizeof(value)));
        return p->insert<Key, T>(key, value, err);
    }

    /**
     * Split a leaf page.
     * If the ancestors has no space for index records,
     * Split will occur recursively.
     *
     * RETURN:
     *   Page pointer for the key to be inserted.
     *   The level is the same as a given page.
     */
    Page *splitLeaf(Page *page, const Key &key) {
        assert(page->isLeaf());
#if 0
        ::printf("splitLeaf: %p\n", page); /* debug */
#endif
        Page *parent = page->parent();
        Page *p0, *p1;
        std::tie(p0, p1) = page->split();
        assert(!p0->empty());
        assert(!p1->empty());
        p0->header().level = 0;
        p1->header().level = 0;
        const Key &key0 = p0->minKey<Key>();
        const Key &key1 = p1->minKey<Key>();

#if 0
        ::printf("splitted %p %p key %u %u\n", p0, p1, key0, key1); /* debug */
#endif
        UNUSED bool ret;
        if (!parent) {
            /* Root */
            assert(page->empty());
            ret = page->insert(key0, p0); assert(ret);
            ret = page->insert(key1, p1); assert(ret);
            p0->header().parent = page;
            p1->header().parent = page;
            page->header().level = 1;
            page->header().parent = nullptr;
        } else {
            Page *parent0 = parent;
            Page *parent1 = parent;
            if (!parent->canInsert(sizeof(key) + sizeof(Page *))) {
                std::tie(parent0, parent1) = splitNonLeaf(parent, key0, key1);
            }
#if 0
            ::printf("parents %p %p\n", parent0, parent1); /* debug */
            parent0->print<Key, Page *>(); /* debug */
            parent1->print<Key, Page *>(); /* debug */
#endif

            ret = parent0->update(key0, p0); assert(ret);
            ret = parent1->insert(key1, p1); assert(ret);
            p0->header().parent = parent0;
            p1->header().parent = parent1;
        }
        return (less_(key, key1)) ? p0 : p1;
    }
    /**
     * Split a non-leaf page.
     * @key0 first key.
     * @key1 second key.
     * RETURN:
     *  1st: splitted branch page where key0 should be inserted.
     *  2nd: splitted branch page where key1 should be inserted.
     */
    std::tuple<Page *, Page *> splitNonLeaf(Page *page, const Key &key0, const Key &key1) {
        assert(!page->isLeaf());
        uint16_t level = page->header().level;
#if 0
        ::printf("%u splitNonLeaf %p\n", level, page); /* debug */
#endif
        Page *parent = page->parent();
        Page *p0, *p1;
        std::tie(p0, p1) = page->split();
        assert(!p0->empty());
        assert(!p1->empty());
        p0->header().level = level;
        p1->header().level = level;
        const Key &k0 = p0->minKey<Key>();
        const Key &k1 = p1->minKey<Key>();

#if 0
        ::printf("%u splitted: %p %p key %u %u\n", level, p0, p1, k0, k1); /* debug */
#endif
        UNUSED bool ret;
        if (!parent) {
            /* Root */
            assert(page->empty());
            ret = page->insert(k0, p0); assert(ret);
            ret = page->insert(k1, p1); assert(ret);
            p0->header().parent = page;
            p1->header().parent = page;
            page->header().level = level + 1;
            page->header().parent = nullptr;
        } else {
            Page *parent0 = parent;
            Page *parent1 = parent;
            if (!parent->canInsert(sizeof(Key) + sizeof(Page *))) {
                std::tie(parent0, parent1) = splitNonLeaf(parent, k0, k1);
#if 0
                ::printf("%u splitNonLeaf Done parent0 %p parent1 %p\n", level, parent0, parent1); /* debug */
#endif
            }
#if 0
            ::printf("%u try to update %p key %u %p\n", level, parent0, key0, p0); /* debug */
            ::printf("%u try to insert %p key %u %p\n", level, parent1, key1, p1); /* debug */
#endif
            parent0->print<Key, Page *>(); /* debug */
            parent1->print<Key, Page *>(); /* debug */
            ret = parent0->update(k0, p0); assert(ret);
            ret = parent1->insert(k1, p1); assert(ret);
            p0->header().parent = parent0;
            p1->header().parent = parent1;
        }

        /* Update parent field of all children. */
        auto it0 = p0->begin();
        while (it0 != p0->end()) {
            Page *child = it0.value<Page *>();
            child->header().parent = p0;
            ++it0;
        }
        auto it1 = p1->begin();
        while (it1 != p1->end()) {
            Page *child = it1.value<Page *>();
            child->header().parent = p1;
            ++it1;
        }

        /* Which splitted page should the key inserted. */
        Page *ret0 = less_(key0, k1) ? p0 : p1;
        Page *ret1 = less_(key1, k1) ? p0 : p1;
#if 0
        ::printf("%u p0 %p p1 %p\n", level, p0, p1); /* debug */
        p0->print<Key, Page *>(); /* debug */
        p1->print<Key, Page *>(); /* debug */
        ::printf("%u end %p %p\n", level, ret0, ret1); /* debug */
#endif
        return std::make_tuple(ret0, ret1);
    }

    void print() const {
        ::printf("---BEGIN-----------------\n");
        printRecursive(&root_);
        ::printf("---END-----------------\n");
    }
    void printRecursive(const Page *p) const {
        if (p->isLeaf()) {
            p->print<Key, T>();
            return;
        }
        p->print<Key, Page *>();

        Page::ConstIterator it = p->cBegin();
        while (it != p->cEnd()) {
            const Page *child = it.value<Page *>();
            printRecursive(child);
            ++it;
        }
    }

    class PageIterator
    {
    protected:
        using MapT = BtreeMap<Key, T, CompareT>;
        using It = PageIterator;
        MapT *mapP_;
        Page *pageP_; /* Nullptr indicates the end. */
    public:
        PageIterator(MapT *mapP, Page *pageP)
            : mapP_(mapP), pageP_(pageP) {
        }
        It &operator=(const It &rhs) {
            mapP_ = rhs.mapP_;
            pageP_ = rhs.pageP_;
            return *this;
        }
        bool operator==(const It &rhs) const { return pageP_ == rhs.pageP_; }
        bool operator!=(const It &rhs) const { return pageP_ != rhs.pageP_; }
        bool operator<(const It &rhs) const {
            if (!pageP_ && !rhs.pageP_) return false; /* both end. */
            if (pageP_ && rhs.pageP_) { /* both valid. */
                Page *p0 = pageP_;
                Page *p1 = rhs.pageP_;
                return mapP_->less_(p0->minKey<Key>(), p1->minKey<Key>());
            }
            return pageP_ != nullptr;
        }
        bool operator<=(const It &rhs) const { return *this == rhs || *this < rhs; }
        bool operator>(const It &rhs) const { return !(*this <= rhs); }
        bool operator>=(const It &rhs) const { return !(*this < rhs); }
        It &operator++() {
            if (pageP_) {
                pageP_ = mapP_->nextPage(pageP_);
            } else {
                /* Cyclic */
                pageP_ = mapP_->leftMostPage();
            }
            return *this;
        }
        It &operator--() {
            if (pageP_) {
                pageP_ = mapP_->prevPage(pageP_);
            } else {
                /* Cyclic */
                pageP_ = mapP_->rightMostPage();
            }
            return *this;
        }
        bool isEnd() const { return pageP_ == nullptr; }
        void print() const {
            ::printf("PageIterator %p\n", pageP_);
        }

        const Page *page() const { return pageP_; }
        Page *page() { return pageP_; }
    };

    PageIterator beginPage() {
        return PageIterator(this, leftMostPage());
    }
    PageIterator endPage() {
        return PageIterator(this, nullptr);
    }

    class ItemIterator
    {
    protected:
        using MapT = const BtreeMap<Key, T, CompareT>;
        using PageIt = MapT::PageIterator;
        using ItInPage = Page::Iterator;
        using It = ItemIterator;

        MapT *mapP_; /* must not be nullptr. */
        PageIt pit_;
        ItInPage it_; /* no meaning if pageIt_ indicates the end. */

    public:
        ItemIterator(MapT *mapP, PageIt pit, ItInPage it)
            : mapP_(mapP), pit_(pit), it_(it) {
            assert(mapP);
        }
        It &operator=(const It &rhs) {
            mapP_ = rhs.mapP_;
            pit_ = rhs.pit_;
            it_ = rhs.it_;
            return *this;
        }
        bool operator==(const It &rhs) const {
            if (pit_.isEnd() && rhs.pit_.isEnd()) {
                /* Both end. */
                return true;
            }
            if (!pit_.isEnd() && !rhs.pit_.isEnd()) {
                /* Both valid. */
                return pit_ == rhs.pit_ && it_ == rhs.it_;
            }
            return false;
        }
        bool operator!=(const It &rhs) const {
            return !(*this == rhs);
        }
        bool operator<(const It &rhs) const {
            if (pit_.isEnd() && rhs.pit_.isEnd()) {
                /* Both end. */
                return false;
            }
            if (!pit_.isEnd() && !rhs.pit_.isEnd()) {
                /* Both valid. */
                if (pit_ == rhs.pit_) {
                    return it_ < rhs.it_;
                } else {
                    return pit_ < rhs.pit_;
                }
            }
            return !pit_.isEnd();
        }
        bool operator<=(const It &rhs) const { return *this == rhs || *this < rhs; }
        bool operator>(const It &rhs) const { return !(*this <= rhs); }
        bool operator>=(const It &rhs) const { return !(*this < rhs); }
        It &operator++() {
            if (pit_.isEnd()) {
                /* Go to the first item cyclically. */
                ++pit_;
                it_ = pit_.page()->begin();
                return *this;
            }
            ++it_;
            if (it_.isEnd()) {
                ++pit_;
                if (!pit_.isEnd()) {
                    it_ = pit_.page()->begin();
                } else {
                    /* The iterator indicates the end. */
                }
            }
            return *this;
        }
        It &operator--() {
            if (pit_.isEnd()) {
                /* Go to the last item cyclically. */
                --pit_;
                it_ = pit_.page()->end();
                --it_;
                assert(!it_.isEnd());
                return *this;
            }
            if (it_.isBegin()) {
                --pit_;
                if (!pit_.isEnd()) {
                    it_ = pit_.page()->end();
                    --it_;
                    assert(!it_.isEnd());
                } else {
                    /* The iterator indicates the end. */
                }
                return *this;
            }
            --it_;
            return *this;
        }
        bool isEnd() const { return pit_.isEnd(); }
        void print() const {
            pit_.print();
            it_.print();
        }

        const Key &key() const {
            assert(!pit_.isEnd());
            assert(!it_.isEnd());
            return it_.key<Key>();
        }
        const T &value() const {
            assert(!pit_.isEnd());
            assert(!it_.isEnd());
            return it_.value<T>();
        }
    };

    ItemIterator begin() {
        PageIterator pit = beginPage();
        return ItemIterator(this, pit, pit.page()->begin());
    }
    ItemIterator end() {
        PageIterator pit = endPage();
        return ItemIterator(this, pit, Page::Iterator(nullptr, 0));
    }

private:
    /**
     * Get leaf page that has a given key.
     */
    Page *searchLeaf(const Key &key) {
        Page *p = &root_;
        while (!p->isLeaf()) {
            p = p->child(key);
        }
        return p;
    }
    const Page *searchLeaf(const Key &key) const {
        const Page *p = &root_;
        while (!p->isLeaf()) p = p->child(key);
        return p;
    }
    /**
     * Next leaf page.
     */
    Page *nextPage(Page *page) {
        return const_cast<Page *>(nextPageC(page));
    }
    const Page *nextPage(const Page *page) const {
        return nextPageC(page);
    }
    const Page *nextPageC(const Page *page) const {
        if (!page) return leftMostPage();
        assert(page->isLeaf());
        if (page->isRoot()) return nullptr;
        const Page *p = page->parent();
        assert(p);
        Key key0 = page->minKey<Key>();

        /* Traverse ancestors. */
        Key key = key0;
        while (true) {
            Page::ConstIterator it = p->lowerBound(key);
            assert(it != p->end());
            ++it;
            if (it != p->end()) {
                p = p->child(it.key<Key>());
                break;
            }
            key = p->minKey<Key>();
            p = p->parent();
            if (p == nullptr) return nullptr;
        }

        /* Traverse descendants. */
        while (!p->isLeaf()) {
            p = p->leftMostChild();
        }
        return p;
    }
    /**
     * Previous leaf page.
     */
    const Page *prevPageC(const Page *page) const {
        if (!page) return rightMostPage();
        assert(page->isLeaf());
        if (page->isRoot()) return nullptr;
        const Page *p = page->parent();
        assert(p);
        Key key0 = page->minKey<Key>();

        /* Traverse ancestors. */
        Key key = key0;
        while (true) {
            Page::ConstIterator it = p->lowerBound(key);
            assert(it != p->end());
            if (it != p->begin()) {
                --it;
                p = p->child(it.key<Key>());
                break;
            }
            key = p->minKey<Key>();
            p = p->parent();
            if (p == nullptr) return nullptr;
        }

        /* Traverse descendants. */
        while (!p->isLeaf()) {
            p = p->rightMostChild();
        }
        return p;
    }
    Page *prevPage(Page *page) {
        return const_cast<Page *>(prevPageC(page));
    }
    const Page *prevPage(const Page *page) const {
        return prevPageC(page);
    }
    /**
     * Right-most leaf page.
     */
    Page *rightMostPage() {
        Page *p = &root_;
        while (!p->isLeaf()) p = p->rightMostChild();
        return p;
    }
    const Page *rightMostPage() const {
        const Page *p = &root_;
        while (!p->isLeaf()) p = p->rightMostChild();
        return p;
    }
    /**
     * Left-most leaf page.
     */
    Page *leftMostPage() {
        Page *p = &root_;
        while (!p->isLeaf()) p = p->leftMostChild();
        return p;
    }
    const Page *leftMostPage() const {
        const Page *p = &root_;
        while (!p->isLeaf()) p = p->leftMostChild();
        return p;
    }
};

} //namespace cybozu

#endif /* B_TREE_MEMORY_HPP */
