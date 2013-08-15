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
constexpr unsigned int PAGE_SIZE = 1024;

/**
 * Comparison function type.
 * returned value:
 *   negative: key0 is less than key1.
 *   0:        key0 equals to key1.
 *   positive: key0 is greater than key1.
 */
using CompareX = int (*)(const void *keyPtr0, uint16_t keySize0, const void *keyPtr1, uint16_t keySize1);

enum class BtreeError : uint8_t
{
    KEY_EXISTS, KEY_NOT_EXISTS, NO_SPACE, INVALID_KEY,
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
    uint16_t totalDataSize; /* total data size in the page. */
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
 *
 * CompareT type must be the same as CompareX.
 */
template <typename CompareT>
class PageX
{
private:
    std::mutex mutex_;
    std::condition_variable cv_;
    Mgl mgl_;

    using Page = PageX<CompareT>;

    /* All persistent data are stored in the page. */
    char *page_;

public:
    explicit PageX() : page_(allocPageStatic()) {
        init();
    }
    virtual ~PageX() noexcept {
        ::free(page_);
    }
    PageX(const Page &rhs) : page_(allocPageStatic()) {
        ::memcpy(page_, rhs.page_, PAGE_SIZE);
    }
    PageX(Page &&rhs) : page_(rhs.page_) {
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
        header().parent = nullptr;
        header().level = uint16_t(-1); /* POISON value. You must set it by yourself. */
        header().totalDataSize = 0;
#ifdef DEBUG
        /* zero-clear except for header area. */
        uint16_t size = PAGE_SIZE - headerEndOff();
        ::memset(page_ + headerEndOff(), 0, size);
#endif
    }
    bool isValid() const {
        if (!(recEndOff() <= stubBgnOff())) return false;
        if (!(stubBgnOff() <= PAGE_SIZE)) return false;
#ifdef DEBUG
        if (totalDataSize() != calcTotalDataSize()) return false;
#endif
        return true;
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
#ifdef DEBUG
        assert(header().totalDataSize == calcTotalDataSize());
#endif
        return header().totalDataSize;
    }
    uint16_t calcTotalDataSize() const {
        uint16_t total = 0;
        for (uint16_t i = 0; i < numStub(); i++) {
            total += keySize(i) + valueSize(i) + sizeof(struct stub);
        }
        return total;
    }
    uint16_t emptySize() const {
        return PAGE_SIZE - headerEndOff();
    }
    bool canInsert(uint16_t size) const {
        return size + sizeof(struct stub) <= freeSpace();
    }
    bool insert(const void *keyPtr0, uint16_t keySize0,
                const void *valuePtr0, uint16_t valueSize0, BtreeError *err = nullptr) {
        /* Key existence check. */
        {
            uint16_t i = lowerBoundStub(keyPtr0, keySize0);
            if (isNormalIndex(i) && CompareT()(keyPtr0, keySize0, keyPtr(i), keySize(i)) == 0) {
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
            int r = CompareT()(keyPtr0, keySize0, keyPtr(i), keySize(i));
            assert(r != 0);
            if (r < 0) break;
            stub(i - 1) = stub(i);
            ++i;
        }
        stub(i - 1).off = recOff;
        stub(i - 1).keySize = keySize0;
        stub(i - 1).valueSize = valueSize0;
        header().totalDataSize += keySize0 + valueSize0 + sizeof(struct stub);

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
     * Update value of a record.
     * RETURN:
     *   true in success.
     *   false if value size is larger than the stored (TODO).
     *         if key does not exist.
     */
    bool update(const void *keyPtr0, uint16_t keySize0, const void *valuePtr0, uint16_t valueSize0, BtreeError *err = nullptr) {
        uint16_t i = lowerBoundStub(keyPtr0, keySize0);
        if (!isNormalIndex(i) || CompareT()(keyPtr0, keySize0, keyPtr(i), keySize(i)) != 0) {
            if (err) *err = BtreeError::KEY_NOT_EXISTS;
            return false;
        }
        return updateStub(i, valuePtr0, valueSize0, err);
    }
    template <typename Key, typename T>
    bool update(const Key &key, const T &value) {
        return update(&key, sizeof(key), &value, sizeof(value));
    }
    bool isLower(const void *keyPtr0, uint16_t keySize0) const {
        assert(numStub() != 0);
        uint16_t i0 = 0;
        return CompareT()(keyPtr0, keySize0, keyPtr(i0), keySize(i0)) < 0;
    }
    template <typename Key>
    bool isLower(const Key &key) const {
        return isLower(&key, sizeof(key));
    }
    bool isUpper(const void *keyPtr0, uint16_t keySize0) const {
        assert(numStub() != 0);
        uint16_t i1 = numStub() - 1;
        return CompareT()(keyPtr(i1), keySize(i1), keyPtr0, keySize0) < 0;
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
        ::printf("Page: %p level %u numRecords %zu headerEndOff %u recEndOff %u stubBgnOff %u parent %p"
                 , this, level(), numRecords(), headerEndOff(), recEndOff(), stubBgnOff(), parent());
        mgl_.print();
        ::printf("\n");
    }
    /**
     * Estimate the effect of gc() and decide to whether we should do gc() or not.
     */
    bool shouldGc() const {
        return totalDataSize() * 2 < emptySize();
    }
    /**
     * Collect garbage.
     */
    void gc() {
        Page p;
        for (size_t i = 0; i < numStub(); i++) {
            UNUSED bool ret;
            ret = p.insert(keyPtr(i), keySize(i), valuePtr(i), valueSize(i));
            assert(ret);
        }
        p.header().parent = header().parent;
        p.header().level = header().level;
        swap(p);

#if 0
        /* debug */
        ::printf("-----gcbgn------\n");
        print();
        p.print();
        ::printf("-----gcend------\n");
#endif
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
     * Swap page_.
     */
    void swap(Page &rhs) {
        char *page = page_;
        page_ = rhs.page_;
        rhs.page_ = page;
    }

    /**
     * Split a page into two pages.
     * The page will be cleared.
     * You must set parent field by yourself after calling this.
     */
    std::pair<Page *, Page *> split(bool isHalfAndHalf = true) {
        Page *p0 = new Page();
        Page *p1 = new Page();
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
        assert(level() == rhs.level());
        
        /* Insert records in the reverse order for efficiency. */
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
    template <typename PageT, typename It>
    class IteratorBase
    {
    protected:
        friend PageX;
        PageT *pageP_; /* can be null. but almost member functions does not work. */
        uint16_t idx_;

    public:
        IteratorBase(PageT *pageP, uint16_t idx)
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
        uint16_t idx() const { return idx_; }
        void updateIdx(uint16_t idx) {
            assert(idx <= pageP_->numStub()); /* can indicate the end. */
            idx_ = idx;
        }
        
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

        PageT *page() { return pageP_; }
        const PageT *page() const { return pageP_; }
        
    };
    template <typename It>
    using BaseC = IteratorBase<const Page, It>;
    
    class ConstIterator : public BaseC<ConstIterator>
    {
    public:
        ConstIterator(const Page *pageP, uint16_t idx)
            : BaseC<ConstIterator>(pageP, idx) {
        }
    };
    template <typename It>
    using Base = IteratorBase<Page, It>;
    class Iterator : public Base<Iterator>
    {
    public:
        Iterator(Page *pageP, uint16_t idx)
            : Base<Iterator>(pageP, idx) {
        }
        /**
         * The iterator will indicates the next record.
         */
        void erase() {
            Base<Iterator>::pageP_->eraseStub(Base<Iterator>::idx_);
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
    Iterator lowerBound(const Key &key) { return lowerBound(&key, sizeof(Key)); }
    template <typename Key>
    ConstIterator lowerBound(const Key &key) const { return lowerBound(&key, sizeof(Key)); }

    Iterator search(const void *keyPtr0, uint16_t keySize0,
                    bool allowLower = false, bool allowUpper = false) {
        uint16_t i = searchStub(keyPtr0, keySize0);
        if (i == UPPER && !allowUpper) {
            i = numStub() - 1; /* the last. */
        } else if (i == LOWER && !allowLower) {
            i = 0;
        } else if (!isNormalIndex(i)) {
            i = numStub(); /* the end. */
        }
        return Iterator(this, i);
    }
    ConstIterator search(const void *keyPtr0, uint16_t keySize0,
                         bool allowLower = false, bool allowUpper = false) const {
        uint16_t i = searchStub(keyPtr0, keySize0);
        if (i == UPPER && !allowUpper) {
            i = numStub() - 1; /* the last. */
        } else if (i == LOWER && !allowLower) {
            i = 0; /* the first. */
        } else if (!isNormalIndex(i)) {
            i = numStub(); /* the end. */
        }
        return ConstIterator(this, i);
    }
    template <typename Key>
    Iterator search(const Key &key, bool allowLower = false, bool allowUpper = false) {
        return search(&key, sizeof(Key), allowLower, allowUpper);
    }
    template <typename Key>
    ConstIterator search(const Key &key, bool allowLower = false, bool allowUpper = false) const {
        return search(&key, sizeof(Key), allowLower, allowUpper);
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

    bool updateKey(Iterator it, const void *keyPtr0, uint16_t keySize0, BtreeError *err = nullptr) {
        return updateKeyStub(it.idx_, keyPtr0, keySize0, err);
    }
    template <typename Key>
    bool updateKey(Iterator it, const Key &key, BtreeError *err = nullptr) {
        return updateKey(it, &key, sizeof(Key), err);
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
            int r = CompareT()(keyPtr0, keySize0, keyPtr(i), keySize(i));
            if (r == 0) return i;
            if (r < 0) i1 = i;
            else i0 = i;
        }
        if (CompareT()(keyPtr(i0), keySize(i0), keyPtr0, keySize0) < 0) {
            assert(CompareT()(keyPtr0, keySize0, keyPtr(i0 + 1), keySize(i0 + 1)) <= 0);
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
            int r = CompareT()(keyPtr0, keySize0, keyPtr(i), keySize(i));
            if (r == 0) return i;
            if (r < 0) i1 = i;
            else i0 = i;
        }
        if (CompareT()(keyPtr(i1), keySize(i1), keyPtr0, keySize0) == 0) {
            return i1;
        } else {
            assert(CompareT()(keyPtr(i0), keySize(i0), keyPtr0, keySize0) <= 0);
            assert(CompareT()(keyPtr0, keySize0, keyPtr(i0 + 1), keySize(i0 + 1)) < 0);
            return i0;
        }
    }
    /**
     * Update a record with a new value.
     *
     * @i stub number.
     * @valuePtr0 value pointer.
     * @valueSize0 value size.
     * @err error pointer.
     *
     * RETURN:
     *   true in success.
     *   false if there is no space to store new value.
     */
    bool updateStub(uint16_t i, const void *valuePtr0, uint16_t valueSize0, BtreeError *err = nullptr) {
        assert(isValidIndex(i));
        uint16_t oldValueSize = valueSize(i);
        if (oldValueSize < valueSize0) {
            if (err) *err = BtreeError::NO_SPACE;
            return false;
        }
        stub(i).valueSize = valueSize0;
        ::memcpy(valuePtr(i), valuePtr0, valueSize0);
        header().totalDataSize -= oldValueSize - valueSize0;
        return true;
    }
    bool isValidIndex(uint16_t i) const {
        return i < numStub();
    }
    /**
     * Update a record with a new key.
     * The specified key must not break the order law in the page.
     */
    bool updateKeyStub(uint16_t i, const void *keyPtr0, uint16_t keySize0, BtreeError *err = nullptr) {
        assert(isValidIndex(i));
        const uint16_t oldKeySize = keySize(i);
        const void *oldValuePtr = valuePtr(i);
        if (oldKeySize < keySize0) {
            if (err) *err = BtreeError::NO_SPACE;
            return false;
        }

        if (0 < i) {
            /* The left key check. */
            if (CompareT()(keyPtr(i - 1), keySize(i - 1), keyPtr0, keySize0) >= 0) {
                if (err) *err = BtreeError::INVALID_KEY;
                return false;
            }
        }
        if (i < numStub() - 1) {
            /* The right key check */
            if (CompareT()(keyPtr0, keySize0, keyPtr(i + 1), keySize(i + 1)) >= 0) {
                if (err) *err = BtreeError::INVALID_KEY;
                return false;
            }
        }

        /* Update key */
        ::memcpy(keyPtr(i), keyPtr0, keySize0);
        if (keySize0 != oldKeySize) {
            /* Shift value data */
            ::memmove(valuePtr(i), oldValuePtr, valueSize(i));
        }
        stub(i).keySize = keySize0;
        header().totalDataSize -= oldKeySize - keySize0;
        return true;
    }
    /**
     * Erase a stub.
     */
    void eraseStub(size_t i) {
        assert(i < numStub());
        header().totalDataSize -= stub(i).keySize + stub(i).valueSize + sizeof(struct stub);
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

#if 0
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
#endif

/**
 * Map structure using B+tree.
 *
 * Key: key type. copyable.
 * Value: value type. copyable.
 */
template <typename Key, typename T,
          class CompareT = std::less<Key> >
class BtreeMap
{
private:
    struct Compare
    {
        int operator()(const void *keyPtr0, UNUSED uint16_t keySize0,
                       const void *keyPtr1, UNUSED uint16_t keySize1) {
            assert(sizeof(Key) == keySize0);
            assert(sizeof(Key) == keySize1);
            const Key &key0 = *reinterpret_cast<const Key *>(keyPtr0);
            const Key &key1 = *reinterpret_cast<const Key *>(keyPtr1);
            if (key0 == key1) return 0;
            if (CompareT()(key0, key1)) return -1;
            return 1;
        }
    };
    using Page = PageX<Compare>;
    Page root_;

public:
    BtreeMap() {
        root_.header().level = 0;
        root_.header().parent = nullptr;
    }
    ~BtreeMap() noexcept {
        try {
            clear();
        } catch (...) {
        }
    }
    bool insert(const Key &key, const T &value, BtreeError *err = nullptr) {
        size_t size = sizeof(key) + sizeof(value);
        assert(size < (2 << 16));

        /* Get the corresponding leaf page. */
        Page *p = searchLeaf(key);
        assert(p->isLeaf());

        if (!p->canInsert(size) && p->shouldGc()) p->gc();
        if (!p->canInsert(size)) p = splitLeaf(p, key);

        assert(p->canInsert(size));
        return p->template insert<Key, T>(key, value, err);
    }
    /**
     * Delete all records by more efficient way.
     */
    void clear() {
        if (!root_.isLeaf()) {
            /* Delete all pages recursively. */
            typename Page::Iterator it = root_.begin();
            while (it != root_.end()) {
                Page *child = it.template value<Page *>();
                deleteRecursive(child);
                it.erase();
            }
        }
        /* Clear the root page and set as a leaf page. */
        root_.clear();
        root_.header().level = 0;
        root_.header().parent = nullptr;
    }
    void print() const {
        ::printf("---BEGIN-----------------\n");
        printRecursive(&root_);
        ::printf("---END-----------------\n");
    }
    void printRecursive(const Page *p) const {
        if (p->isLeaf()) {
            p->template print<Key, T>();
            return;
        }
        p->template print<Key, Page *>();

        typename Page::ConstIterator it = p->template cBegin();
        while (it != p->cEnd()) {
            const Page *child = it.template value<Page *>();
            printRecursive(child);
            ++it;
        }
    }

    /**
     * Leaf page iterator.
     */
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
            if (pageP) assert(pageP->isLeaf());
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
                return mapP_->CompareT()(
                    p0->template minKey<Key>(), p1->template minKey<Key>());
            }
            return pageP_ != nullptr;
        }
        bool operator<=(const It &rhs) const { return *this == rhs || *this < rhs; }
        bool operator>(const It &rhs) const { return !(*this <= rhs); }
        bool operator>=(const It &rhs) const { return !(*this < rhs); }
        It &operator++() {
            Page *p = pageP_;
            if (p) {
                p = mapP_->nextPage(p);
            } else {
                /* Cyclic */
                p = mapP_->leftMostPage();
            }
            assert(pageP_ != p);
            pageP_ = p;
            return *this;
        }
        It &operator--() {
            Page *p = pageP_;
            if (p) {
                p = mapP_->prevPage(p);
            } else {
                /* Cyclic */
                p = mapP_->rightMostPage();
            }
            assert(pageP_ != p);
            pageP_ = p;
            return *this;
        }
        bool isEnd() const { return pageP_ == nullptr; }
        void print() const {
            ::printf("PageIterator %p\n", pageP_);
        }

        const Page *page() const { return pageP_; }
        Page *page() { return pageP_; }
    };

    class ConstPageIterator : public PageIterator
    {
    private:
        using MapT = BtreeMap<Key, T, CompareT>;
        using It = ConstPageIterator;
    public:
        ConstPageIterator(const MapT *mapP, const Page *pageP)
            : PageIterator(const_cast<MapT *>(mapP), const_cast<Page *>(pageP)) {
        }
    };

    PageIterator beginPage() {
        return PageIterator(this, leftMostPage());
    }
    PageIterator endPage() {
        return PageIterator(this, nullptr);
    }
    ConstPageIterator beginPage() const {
        return ConstPageIterator(this, leftMostPage());
    }
    ConstPageIterator endPage() const {
        return ConstPageIterator(this, nullptr);
    }

    /**
     * Item iterator.
     */
    class ItemIterator
    {
    protected:
        using MapT = BtreeMap<Key, T, CompareT>;
        using PageIt = MapT::PageIterator;
        using ItInPage = typename Page::Iterator;
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
                nextPage();
                return *this;
            }
            ++it_;
            if (it_.isEnd()) nextPage();
            return *this;
        }
        It &operator--() {
            if (pit_.isEnd()) {
                /* Go to the last item cyclically. */
                prevPage();
                return *this;
            }
            if (it_.isBegin()) {
                prevPage();
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
        /**
         * Erase the item.
         * The iterator will indicate the next item.
         */
        void erase() {
            assert(!isEnd());
            Key lastKey = it_.template key<Key>();
            Page *page = it_.page();

            if (it_.page()->numRecords() == 1) {
                typename Page::Iterator it = it_;
                nextPage(); /* Do not call this for empty pages. */
                it.erase();
                assert(page->empty());
                mapP_->deleteEmptyPage(page, lastKey);
                mapP_->liftUp();
                return;
            }
            bool isBegin = it_.isBegin();
            it_.erase();
            assert(!it_.page()->empty());

            UNUSED bool isEnd = it_.isEnd();
            UNUSED Key key;
            if (!isEnd) key = it_.template key<Key>();
            if (isBegin) mapP_->updateMinKey(it_.page());
            it_ = mapP_->tryMerge(it_);
            if (isEnd) assert(it_.isEnd());
            else assert(key == it_.template key<Key>());
            mapP_->liftUp();
        }
        const Key &key() const {
            assert(!pit_.isEnd());
            assert(!it_.isEnd());
            return it_.template key<Key>();
        }
        const T &value() const {
            assert(!pit_.isEnd());
            assert(!it_.isEnd());
            return it_.template value<T>();
        }

    private:
        void nextPage() {
            ++pit_;
            if (!pit_.isEnd()) {
                it_ = pit_.page()->begin();
            } else {
                /* The iterator indicates the end. */
            }
        }
        void prevPage() {
            --pit_;
            if (!pit_.isEnd()) {
                it_ = pit_.page()->end();
                --it_;
                assert(!it_.isEnd());
            } else {
                /* The iterator indicates the end. */
            }
        }
    };

    ItemIterator beginItem() {
        PageIterator pit = beginPage();
        return ItemIterator(this, pit, pit.page()->begin());
    }
    ItemIterator endItem() {
        PageIterator pit = endPage();
        return ItemIterator(this, pit, typename Page::Iterator(nullptr, 0));
    }
    ItemIterator lowerBound(const Key &key) {
        Page *page = searchLeaf(key);
        assert(page);
        typename Page::Iterator it = page->lowerBound(key);
        if (it.isEnd()) {
            /* The record is the first one of the next page
               if the next page exists. */
            page = nextPage(page);
            if (page) {
                it = page->lowerBound(key);
            } else {
                /* Not found */
            }
        }
        PageIterator pit(this, page);
        if (it.isEnd()) {
            return ItemIterator(this, endPage(), it);
        } else {
            return ItemIterator(this, pit, it);
        }
    }
    /**
     * Behave like std::map::erase().
     */
    ItemIterator erase(ItemIterator it) {
        it.erase();
        return it;
    }
    /**
     * Delete a record.
     */
    bool erase(const Key &key) {
        ItemIterator it = lowerBound(key);
        if (it.isEnd()) return false;
        if (it.key() != key) return false;
        it.erase();
        return true;
    }
    bool isValid() const {
        return isValid(&root_);
    }
    /**
     * Validate.
     *
     * For all non-leaf pages, keys of all its children is within the range.
     */
    bool isValid(const Page *p) const {
        assert(p);
        if (p->isLeaf()) return p->isValid();
        uint16_t level = p->level();
        typename Page::ConstIterator it = p->begin();
        while (it != p->end()) {
            const Page *child = it.template value<const Page *>();
            assert(child);
            if (!(child->level() + 1 == level)) {
                ::printf("error: child level is not valid.\n");
                return false;
            }
            if (!(child->parent() == p)) {
                ::printf("error: child's parent is not valid.\n");
                return false;
            }
            if (!child->isValid()) {
                ::printf("error: child is not valid.\n");
                return false;
            }
            if (child->empty()) {
                ::printf("error: child is empty.\n");
                return false;
            }
            if (!isValid(child)) {
                return false;
            }
            ++it;
        }
        return true;
    }
    bool empty() const {
        return root_.isLeaf() && root_.empty();
    }
    size_t size() const {
        size_t total = 0;
        ConstPageIterator it = beginPage();
        while (it != endPage()) {
            //::printf("size: page: %p\n", it.page());
            total += it.page()->numRecords();
            ++it;
        }
        return total;
    }
private:
    /**
     * Split a leaf page.
     * If the ancestors has no space for index records,
     * Split will occur recursively.
     *
     * @page page that will be splitted. it will be deleted.
     * @key a key to insert.
     *
     * RETURN:
     *   Page pointer for the key to be inserted.
     *   The level is the same as a given page.
     */
    Page *splitLeaf(Page *page, const Key &key) {
        assert(page->isLeaf());
#if 0
        ::printf("splitLeaf: %p (level %u)\n", page, page->level()); /* debug */
        page->print<Key, T>();
#endif

        Page *parent = page->parent();
        //::printf("parent %p\n", parent); /* debug */
        Page *p0, *p1;
        std::tie(p0, p1) = page->split();
        assert(!p0->empty());
        assert(!p1->empty());
        p0->header().level = 0;
        p1->header().level = 0;
        const Key &k0 = p0->template minKey<Key>();
        const Key &k1 = p1->template minKey<Key>();

#if 0
        ::printf("splitted %p %p key %u %u\n", p0, p1, k0, k1); /* debug */
#endif
        UNUSED bool ret;
        if (!parent) {
            /* Root */
            assert(page == &root_);
            //::printf("page %p\n", page); /* debug */
            assert(page->empty());
            ret = page->insert(k0, p0); assert(ret);
            ret = page->insert(k1, p1); assert(ret);
            p0->header().parent = page;
            p1->header().parent = page;
            page->header().level = 1;
            //::printf("root level %u (splitLeaf)\n", page->level()); /* debug */
        } else {
            Page *parent0 = parent;
            Page *parent1 = parent;
            const uint16_t recSize = sizeof(Key) + sizeof(Page *);
            //parent->print<Key, Page *>(); /* debug */
            if (!parent->canInsert(recSize)) {
                parent->gc();
            }
            if (!parent->canInsert(recSize)) {
                //::printf("try to call splitNonLeaf()\n"); /* debug */
                std::tie(parent0, parent1) = splitNonLeaf(parent, k0, k1);
            }
#if 0
            ::printf("parents %p %p\n", parent0, parent1); /* debug */
            parent0->print<Key, Page *>(); /* debug */
            parent1->print<Key, Page *>(); /* debug */
#endif

            typename Page::Iterator it = parent0->search(k0);
            assert(!it.isEnd());
            assert(it.template value<Page *>() == page);
            const Key &k2 = it.template key<Key>();
            if (k2 == k0) {
                ret = parent0->update(k0, p0); assert(ret);
            } else {
                /* This is the case of left-most,
                   or the case left-most-key in the page are deleted. */
                ret = parent0->erase(k2); assert(ret);
                ret = parent0->insert(k0, p0); assert(ret);
            }
            /* GC may be reuiqred when
               parent0 and parent1 is the same page.
               After GC, the insertion will success definitely.
            */
            if (!parent1->canInsert(recSize)) {
                parent1->gc();
            }
            ret = parent1->insert(k1, p1); assert(ret);
            p0->header().parent = parent0;
            p1->header().parent = parent1;
            delete page;
        }
        return (CompareT()(key, k1)) ? p0 : p1;
    }
    /**
     * Split a non-leaf page.
     * @page page that will be splitted. it will be deleted.
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
        const Key &k0 = p0->template minKey<Key>();
        const Key &k1 = p1->template minKey<Key>();

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
            //::printf("root level %u (splitNonLeaf)\n", page->level()); /* debug */
            page->header().parent = nullptr;
        } else {
            Page *parent0 = parent;
            Page *parent1 = parent;
            if (!parent->canInsert(sizeof(Key) + sizeof(Page *))) {
                parent->gc();
            }
            if (!parent->canInsert(sizeof(Key) + sizeof(Page *))) {
                std::tie(parent0, parent1) = splitNonLeaf(parent, k0, k1);
#if 0
                ::printf("%u splitNonLeaf Done parent0 %p parent1 %p\n", level, parent0, parent1); /* debug */
#endif
            }
#if 0
            ::printf("%u try to update %p key %u %p\n", level, parent0, key0, p0); /* debug */
            ::printf("%u try to insert %p key %u %p\n", level, parent1, key1, p1); /* debug */
            parent0->print<Key, Page *>(); /* debug */
            parent1->print<Key, Page *>(); /* debug */
#endif
            typename Page::Iterator it = parent0->search(k0);
            assert(!it.isEnd());
            assert(it.template value<Page *>() == page);
            const Key &k2 = it.template key<Key>();
            if (k2 == k0) {
                ret = parent0->update(k2, p0); assert(ret);
            } else {
                /* This is the case of left-most,
                   or the case left-most-key in the page are deleted. */
                ret = parent0->erase(k2); assert(ret);
                ret = parent0->insert(k0, p0); assert(ret);
            }
            /* GC may be reuiqred when
               parent0 and parent1 is the same page.
               After GC, the insertion will success definitely.
            */
            if (!parent1->canInsert(sizeof(Key) + sizeof(Page *))) {
                parent1->gc();
            }
            ret = parent1->insert(k1, p1); assert(ret);
            p0->header().parent = parent0;
            p1->header().parent = parent1;
            delete page;
        }

        /* Update parent field of all children. */
        auto it0 = p0->begin();
        while (it0 != p0->end()) {
            Page *child = it0.template value<Page *>();
            child->header().parent = p0;
            ++it0;
        }
        auto it1 = p1->begin();
        while (it1 != p1->end()) {
            Page *child = it1.template value<Page *>();
            child->header().parent = p1;
            ++it1;
        }

        /* Which splitted page should the key inserted. */
        Page *ret0 = CompareT()(key0, k1) ? p0 : p1;
        Page *ret1 = CompareT()(key1, k1) ? p0 : p1;
#if 0
        ::printf("%u p0 %p p1 %p\n", level, p0, p1); /* debug */
        p0->template print<Key, Page *>(); /* debug */
        p1->template print<Key, Page *>(); /* debug */
        ::printf("%u end %p %p\n", level, ret0, ret1); /* debug */
#endif
        return std::make_tuple(ret0, ret1);
    }
    /**
     * Get leaf page that has a given key.
     * RETURN:
     *   never nullptr.
     */
    Page *searchLeaf(const Key &key) {
        Page *p = &root_;
        while (!p->isLeaf()) p = p->child(key);
        return p;
    }
    const Page *searchLeaf(const Key &key) const {
        const Page *p = &root_;
        while (!p->isLeaf()) p = p->child(key);
        return p;
    }

    /**
     * Get a parent record.
     */
    typename Page::ConstIterator parentRecord(const Page *page) const {
        assert(page);
        assert(!page->empty());
        const Page *parent = page->parent();
        assert(parent);

        const Key &key0 = page->template minKey<Key>();
        typename Page::ConstIterator it = parent->search(key0);

        /* The key of parent record may be less than the key0
           because some records may have been deleted from the page.
           If so, the next key must be the exact record. */
        if (it.template value<const Page *>() != page) {
            ++it;
            assert(!it.isEnd());
        }
        assert(it.template value<const Page *>() == page);
        return it;
    }
    typename Page::Iterator parentRecord(Page *page) {
        assert(page);
        assert(!page->empty());
        Page *parent = page->parent();
        assert(parent);

        const Key &key0 = page->template minKey<Key>();
        typename Page::Iterator it = parent->search(key0);

        /* The key of parent record may be less than the key0
           because some records may have been deleted from the page.
           If so, the next key must be the exact record. */
        if (it.template value<const Page *>() != page) {
            ++it;
            assert(!it.isEnd());
        }
        assert(it.template value<const Page *>() == page);
        return it;
    }
    /**
     * Next leaf page.
     * This assumes the page is not empty.
     */
    const Page *nextPageC(const Page *page) const {
        if (!page) return leftMostPage();
        assert(page->isLeaf());
        if (page->isRoot()) return nullptr;
        assert(!page->empty());
        const Page *p = page;

        /* Traverse ancestors. */
        while (true) {
            typename Page::ConstIterator it = parentRecord(p);
            ++it;
            if (it != p->parent()->end()) {
                p = it.template value<Page *>(); /* child */
                break;
            }
            p = p->parent();
            if (p->parent() == nullptr) return nullptr;
        }

        /* Traverse descendants. */
        while (!p->isLeaf()) {
            p = p->leftMostChild();
        }
        assert(page != p);
        return p;
    }
    /**
     * Previous leaf page.
     * This assumes the page is not empty.
     */
    const Page *prevPageC(const Page *page) const {
        if (!page) return rightMostPage();
        assert(page->isLeaf());
        if (page->isRoot()) return nullptr;
        assert(!page->empty());
        const Page *p = page;

        /* Traverse ancestors. */
        while (true) {
            typename Page::ConstIterator it = parentRecord(p);
            if (it != p->parent()->begin()) {
                --it;
                p = it.template value<Page *>(); /* child */
                break;
            }
            p = p->parent();
            if (p->parent() == nullptr) return nullptr;
        }

        /* Traverse descendants. */
        while (!p->isLeaf()) {
            p = p->rightMostChild();
        }
        assert(page != p);
        return p;
    }
    Page *nextPage(Page *page) {
        return const_cast<Page *>(nextPageC(page));
    }
    const Page *nextPage(const Page *page) const {
        return nextPageC(page);
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
    /**
     * Delete a page and its descendants recursively.
     * This is top down.
     */
    void deleteRecursive(Page *page) {
        //::printf("deleteRecursive: %p\n", page);
        assert(page);
        if (page->isLeaf()) {
            delete page;
            return;
        }
        typename Page::Iterator it = page->begin();
        while (it != page->end()) {
            Page *child = it.template value<Page *>();
            deleteRecursive(child);
            it.erase();
        }
        assert(page->empty());
        delete page;
    }
    /**
     * Delete an empty page.
     * This will remove ancestors recursively if they will be also empty.
     * Root page will not be deleted even if empty.
     * This is bottom up.
     *
     * @page target page to delete.
     * @key the last key deleted from the page.
     */
    void deleteEmptyPage(Page *page, const Key &key) {
        assert(page);
        assert(page->empty());
        if (page->isRoot()) return;

        /* Delete the correspoding record from the parent. */
        Page *parent = page->parent();
        assert(parent);
        typename Page::Iterator it = parent->search(key);
        assert(it.template value<Page *>() == page);
        bool isBegin = it.isBegin();
        it.erase();

        delete page;
        page = nullptr;

        /* Call it recursively is necessary. */
        if (parent->empty()) {
            deleteEmptyPage(parent, key);
        } else if (isBegin) {
            updateMinKey(parent);
        }
    }
    /**
     * Modify the key of ancestors for the minimum key of 
     * a specified page.
     */
    void updateMinKey(Page *page) {
        assert(page);
        assert(!page->empty());
        if (page->isRoot()) return;

        Page *parent = page->parent();
        assert(parent);
        const Key &key = page->template minKey<Key>();
        typename Page::Iterator it = parentRecord(page);
        UNUSED bool ret = parent->updateKey(it, key);
        assert(ret);

        if (it.isBegin()) {
            /* Recursive call. */
            updateMinKey(parent);
        }
    }
    /**
     * Try merge the page and its left page.
     */
    typename Page::Iterator tryMerge(typename Page::Iterator it) {
        Page *page = it.page();
        assert(page);
        assert(!page->empty());
        if (page->isRoot()) return it;
        if (page->emptySize() < page->totalDataSize() * 3) {
            /* No need to merge. */
            return it;
        }
        typename Page::Iterator it0 = parentRecord(page);
        if (it0.isBegin()) return it;
        --it0;
        Page *leftPage = it0.template value<Page *>();
        if (page->emptySize() < leftPage->totalDataSize() + page->totalDataSize()) {
            /* No space to merge. */
            return it;
        }
        if (page->freeSpace() < leftPage->totalDataSize()) {
            page->gc();
        }
#if 0
        ::printf("do really merge (level %u)\n", page->level()); /* debug */
#endif
        assert(leftPage->totalDataSize() <= page->freeSpace());
        if (!leftPage->isLeaf()) {
            /* Update parent firld of the children of the old left page. */
            typename Page::Iterator it1 = leftPage->begin();
            while (!it1.isEnd()) {
                Page *child = it1.template value<Page *>();
                child->header().parent = page;
                ++it1;
            }
        }            
        uint16_t n = leftPage->numRecords();
        UNUSED bool ret = page->merge(*leftPage);
        assert(ret);
        delete leftPage;
        it.updateIdx(it.idx() + n);
        Key key = it0.template key<Key>();
        it0.erase(); /* delete leftPage's record in the parent page. */
        assert(it0.template value<Page *>() == page);
        /* Update rightPage's key with leftPage's one. */
        ret = it0.page()->updateKey(it0, key); 
        assert(ret);
        tryMerge(it0); /* recursive call. */
        return it;
    }
    /**
     * Shrink the depth if possible.
     */
    void liftUp() {
        //::printf("liftUp\n"); /* debug */
        Page *p = &root_;
        while (!p->isLeaf() && p->numRecords() == 1) {
            UNUSED uint16_t level = p->level();
            Page *child = p->leftMostChild();
            p->swap(*child);
            p->header().parent = nullptr;
            assert(level == p->level() + 1);
            delete child;
        }
        if (!p->isLeaf()) {
            /* Update childrens' parent to the root */
            typename Page::Iterator it = p->begin();
            while (it != p->end()) {
                Page *child = it.template value<Page *>();
                assert(child);
                child->header().parent = p;
                ++it;
            }
        }
    }
};

} //namespace cybozu

#endif /* B_TREE_MEMORY_HPP */
