/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#undef NDEBUG

#define private public

#include "record/rm.h"
#include "storage/buffer_pool_manager.h"

#undef private

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iostream>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <thread>  // NOLINT
#include <unordered_map>
#include <vector>

#include "gtest/gtest.h"
#include "replacer/lru_replacer.h"
#include "storage/disk_manager.h"

const std::string TEST_DB_NAME = "BufferPoolManagerTest_db";  // 以数据库名作为根目录
const std::string TEST_FILE_NAME = "basic";                   // 测试文件的名字
const std::string TEST_FILE_NAME_CCUR = "concurrency";        // 测试文件的名字
const std::string TEST_FILE_NAME_BIG = "bigdata";             // 测试文件的名字
constexpr int MAX_FILES = 32;
constexpr int MAX_PAGES = 128;
constexpr size_t TEST_BUFFER_POOL_SIZE = MAX_FILES * MAX_PAGES;

// 全局 disk_manager 和 buffer_pool_manager（与 rmdb.cpp 中定义一致，供单元测试链接使用）
DiskManager disk_manager;
BufferPoolManager buffer_pool_manager;

std::unordered_map<int, char *> mock;  // fd -> buffer

char *mock_get_page(int fd, int page_no) { return &mock[fd][page_no * PAGE_SIZE]; }

void check_disk(int fd, int page_no) {
    char buf[PAGE_SIZE];
    disk_manager.read_page(fd, page_no, buf, PAGE_SIZE);
    char *mock_buf = mock_get_page(fd, page_no);
    assert(memcmp(buf, mock_buf, PAGE_SIZE) == 0);
}

void check_disk_all() {
    for (auto &file : mock) {
        int fd = file.first;
        for (int page_no = 0; page_no < MAX_PAGES; page_no++) {
            check_disk(fd, page_no);
        }
    }
}

void check_cache(int fd, int page_no) {
    Page *page = buffer_pool_manager.fetch_page(PageId{fd, page_no});
    char *mock_buf = mock_get_page(fd, page_no);  // &mock[fd][page_no * PAGE_SIZE];
    assert(memcmp(page->get_data(), mock_buf, PAGE_SIZE) == 0);
    buffer_pool_manager.unpin_page(PageId{fd, page_no}, false);
}

void check_cache_all() {
    for (auto &file : mock) {
        int fd = file.first;
        for (int page_no = 0; page_no < MAX_PAGES; page_no++) {
            check_cache(fd, page_no);
        }
    }
}

void rand_buf(int size, char *buf) {
    for (int i = 0; i < size; i++) {
        int rand_ch = rand() & 0xff;
        buf[i] = rand_ch;
    }
}

int rand_fd() {
    assert(mock.size() == MAX_FILES);
    int fd_idx = rand() % MAX_FILES;
    auto it = mock.begin();
    for (int i = 0; i < fd_idx; i++) {
        it++;
    }
    return it->first;
}

struct rid_hash_t {
    size_t operator()(const Rid &rid) const { return (rid.page_no << 16) | rid.slot_no; }
};

struct rid_equal_t {
    bool operator()(const Rid &x, const Rid &y) const { return x.page_no == y.page_no && x.slot_no == y.slot_no; }
};

void check_equal(const RmFileHandle *file_handle,
                 const std::unordered_map<Rid, std::string, rid_hash_t, rid_equal_t> &mock) {
    // Test all records
    for (auto &entry : mock) {
        Rid rid = entry.first;
        auto mock_buf = (char *)entry.second.c_str();
        auto [tuple_meta, rec] = file_handle->get_record(rid);
        assert(memcmp(mock_buf, rec->data, file_handle->file_hdr_.record_size) == 0);
    }
    // Randomly get record
    for (int i = 0; i < 10; i++) {
        Rid rid = {.page_no = 1 + rand() % (file_handle->file_hdr_.num_pages - 1),
                   .slot_no = rand() % file_handle->file_hdr_.num_records_per_page};
        bool mock_exist = mock.count(rid) > 0;
        bool rm_exist = true;
        try {
            file_handle->get_record(rid);
        } catch (const RecordNotFoundError &e) {
            rm_exist = false;
        }
        assert(rm_exist == mock_exist);
    }
    // Test RM scan
    size_t num_records = 0;
    for (RmScan scan(file_handle); !scan.is_end(); scan.next()) {
        assert(mock.count(scan.rid()) > 0);
        auto [tuple_meta, rec] = file_handle->get_record(scan.rid());
        assert(memcmp(rec->data, mock.at(scan.rid()).c_str(), file_handle->file_hdr_.record_size) == 0);
        num_records++;
    }
    assert(num_records == mock.size());
}

/** 注意：每个测试点只测试了单个文件！
 * 对于每个测试点，先创建和进入目录TEST_DB_NAME
 * 然后在此目录下创建和打开文件TEST_FILE_NAME_BIG，记录其文件描述符fd */

class BigStorageTest : public ::testing::Test {
   public:
    std::unique_ptr<DiskManager> disk_manager_;
    int fd_ = -1;

   public:
    // 在每个测试前创建独立的 DiskManager 并准备测试文件
    void SetUp() override {
        ::testing::Test::SetUp();
        // For each test, we create a new DiskManager
        disk_manager_ = std::make_unique<DiskManager>();
        // 如果测试目录不存在，则先创建测试目录
        if (!disk_manager_->is_dir(TEST_DB_NAME)) {
            disk_manager_->create_dir(TEST_DB_NAME);
        }
        assert(disk_manager_->is_dir(TEST_DB_NAME));
        // 进入测试目录
        if (chdir(TEST_DB_NAME.c_str()) < 0) {
            throw UnixError();
        }
        // 如果测试文件存在，则先删除原文件（最后留下来的文件存的是最后一个测试点的数据）
        if (disk_manager_->is_file(TEST_FILE_NAME_BIG)) {
            disk_manager_->destroy_file(TEST_FILE_NAME_BIG);
        }
        // 创建测试文件
        disk_manager_->create_file(TEST_FILE_NAME_BIG);
        assert(disk_manager_->is_file(TEST_FILE_NAME_BIG));
        // 打开测试文件
        fd_ = disk_manager_->open_file(TEST_FILE_NAME_BIG);
        assert(fd_ != -1);
    }

    // This function is called after every test.
    void TearDown() override {
        disk_manager_->close_file(fd_);
        // disk_manager_->destroy_file(TEST_FILE_NAME_BIG);  // you can choose to delete the file

        // 返回上一层目录
        if (chdir("..") < 0) {
            throw UnixError();
        }
        assert(disk_manager_->is_dir(TEST_DB_NAME));
    };
};

// 测试 LRU-C 页面替换策略的基本行为：
//   - unpin 将帧加入 LRU 链表，重复 unpin 更新引用位
//   - victim 优先淘汰干净页链表尾部，获取最久未使用的帧
//   - pin 将被淘汰的帧从链表中移除（已淘汰的帧 pin 无效果）
//   - 再次 unpin 后，帧回到链表头部继续参与淘汰
TEST(LRUReplacerTest, SampleTest) {
    LRUReplacer lru_replacer;

    // 1. 向替换器中加入6个帧（1 被 unpin 两次，验证重复处理）
    lru_replacer.unpin(1);
    lru_replacer.unpin(2);
    lru_replacer.unpin(3);
    lru_replacer.unpin(4);
    lru_replacer.unpin(5);
    lru_replacer.unpin(6);
    lru_replacer.unpin(1);
    EXPECT_EQ(6, lru_replacer.Size());

    // 2. 淘汰最久未使用的3个帧，顺序应为 1→2→3
    int value;
    lru_replacer.victim(&value);
    EXPECT_EQ(1, value);
    lru_replacer.victim(&value);
    EXPECT_EQ(2, value);
    lru_replacer.victim(&value);
    EXPECT_EQ(3, value);

    // 3. 固定已淘汰的3（无效果）和仍在链表中的4
    lru_replacer.pin(3);
    lru_replacer.pin(4);
    EXPECT_EQ(2, lru_replacer.Size());

    // 4. 再次释放4，验证其引用位被正确设置
    lru_replacer.unpin(4);

    // 5. 继续淘汰，顺序应为 5→6→4
    lru_replacer.victim(&value);
    EXPECT_EQ(5, value);
    lru_replacer.victim(&value);
    EXPECT_EQ(6, value);
    lru_replacer.victim(&value);
    EXPECT_EQ(4, value);
}

// 缓冲池管理器单线程测试夹具：
// 每个测试点在 TEST_DB_NAME 目录下创建 TEST_FILE_NAME 文件，
// 验证 new_page / fetch_page / unpin_page / flush_all_pages 等基本操作
class BufferPoolManagerTest : public ::testing::Test {
   public:
    int fd_ = -1;

   public:
    void SetUp() override {
        ::testing::Test::SetUp();
        // 如果测试目录不存在，则先创建测试目录
        if (!disk_manager.is_dir(TEST_DB_NAME)) {
            disk_manager.create_dir(TEST_DB_NAME);
        }
        assert(disk_manager.is_dir(TEST_DB_NAME));
        // 进入测试目录
        if (chdir(TEST_DB_NAME.c_str()) < 0) {
            throw UnixError();
        }
        // 如果测试文件存在，则先删除原文件（最后留下来的文件存的是最后一个测试点的数据）
        if (disk_manager.is_file(TEST_FILE_NAME)) {
            disk_manager.destroy_file(TEST_FILE_NAME);
        }
        // 创建测试文件
        disk_manager.create_file(TEST_FILE_NAME);
        assert(disk_manager.is_file(TEST_FILE_NAME));
        // 打开测试文件
        fd_ = disk_manager.open_file(TEST_FILE_NAME);
        assert(fd_ != -1);
    }

    // This function is called after every test.
    void TearDown() override {
        buffer_pool_manager.delete_all_pages(fd_);  // 清理缓冲池中残留的页面，防止 fd 复用污染
        disk_manager.close_file(fd_);
        // disk_manager.destroy_file(TEST_FILE_NAME);  // you can choose to delete the file

        // 返回上一层目录
        if (chdir("..") < 0) {
            throw UnixError();
        }
        assert(disk_manager.is_dir(TEST_DB_NAME));
    };
};

// 测试缓冲池管理器的基本 CRUD 流程：
//   1. 空池创建页面（new_page）
//   2. 页面读写与数据持久化
//   3. 批量页面创建与 eviction 淘汰机制
//   4. unpin → fetch 验证数据一致性
//   5. flush_all_pages 将脏页写回磁盘
TEST_F(BufferPoolManagerTest, SampleTest) {
    auto bpm = std::make_unique<BufferPoolManager>();
    int fd = BufferPoolManagerTest::fd_;
    PageId page_id_temp = {.fd = fd, .page_no = INVALID_PAGE_ID};
    auto *page0 = bpm->new_page(&page_id_temp);

    // 1. 空池创建第一页，page_no 从 0 开始
    ASSERT_NE(nullptr, page0);
    EXPECT_EQ(0, page_id_temp.page_no);

    // 2. 写入数据后立即读出，验证读写正确
    snprintf(page0->get_data(), sizeof(page0->get_data()), "Hello");
    EXPECT_EQ(0, strcmp(page0->get_data(), "Hello"));

    // 3. 批量创建100页，验证连续分配能力
    const int num_pages = 100;
    std::vector<PageId> page_ids;
    page_ids.push_back({fd, 0});
    for (int i = 1; i < num_pages; ++i) {
        page_id_temp = {.fd = fd, .page_no = INVALID_PAGE_ID};
        EXPECT_NE(nullptr, bpm->new_page(&page_id_temp));
        page_ids.push_back(page_id_temp);
    }

    // 4. unpin 前5页 → 创建4个新页（触发替换器的淘汰机制）→ 重新 fetch page[0] 验证数据仍存在
    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(true, bpm->unpin_page(page_ids[i], true));
    }
    for (int i = 0; i < 4; ++i) {
        page_id_temp = {.fd = fd, .page_no = INVALID_PAGE_ID};
        EXPECT_NE(nullptr, bpm->new_page(&page_id_temp));
    }
    page0 = bpm->fetch_page(page_ids[0]);
    EXPECT_EQ(0, strcmp(page0->get_data(), "Hello"));
    EXPECT_EQ(true, bpm->unpin_page(page_ids[0], true));

    // 5. 再次 fetch 同一页，验证 unpin 后数据未丢失
    page0 = bpm->fetch_page(page_ids[0]);
    ASSERT_NE(nullptr, page0);
    EXPECT_EQ(0, strcmp(page0->get_data(), "Hello"));
    EXPECT_EQ(true, bpm->unpin_page(page_ids[0], true));

    // 6. 将所有脏页刷盘
    bpm->flush_all_pages(fd);
}

/** 注意：每个测试点只测试了单个文件！
 * 对于每个测试点，先创建和进入目录TEST_DB_NAME
 * 然后在此目录下创建和打开文件TEST_FILE_NAME_CCUR，记录其文件描述符fd */

// 缓冲池管理器并发测试夹具：
// 多个线程共享同一个 BufferPoolManager，同时执行 new_page / fetch_page / delete_page，
// 验证并发场景下的页面分配、读取、删除和刷盘是否正确
class BufferPoolManagerConcurrencyTest : public ::testing::Test {
   public:
    int fd_ = -1;

   public:
    void SetUp() override {
        ::testing::Test::SetUp();
        // 如果测试目录不存在，则先创建测试目录
        if (!disk_manager.is_dir(TEST_DB_NAME)) {
            disk_manager.create_dir(TEST_DB_NAME);
        }
        assert(disk_manager.is_dir(TEST_DB_NAME));
        // 进入测试目录
        if (chdir(TEST_DB_NAME.c_str()) < 0) {
            throw UnixError();
        }
        // 如果测试文件存在，则先删除原文件（最后留下来的文件存的是最后一个测试点的数据）
        if (disk_manager.is_file(TEST_FILE_NAME_CCUR)) {
            disk_manager.destroy_file(TEST_FILE_NAME_CCUR);
        }
        // 创建测试文件
        disk_manager.create_file(TEST_FILE_NAME_CCUR);
        assert(disk_manager.is_file(TEST_FILE_NAME_CCUR));
        // 打开测试文件
        fd_ = disk_manager.open_file(TEST_FILE_NAME_CCUR);
        assert(fd_ != -1);
    }

    // This function is called after every test.
    void TearDown() override {
        buffer_pool_manager.delete_all_pages(fd_);  // 清理缓冲池中残留的页面，防止 fd 复用污染
        disk_manager.close_file(fd_);
        // disk_manager.destroy_file(TEST_FILE_NAME_CCUR);  // you can choose to delete the file

        // 返回上一层目录
        if (chdir("..") < 0) {
            throw UnixError();
        }
        assert(disk_manager.is_dir(TEST_DB_NAME));
    };
};

// 并发测试：5 个线程共享一个 BufferPoolManager，重复 50 轮。
// 每个线程独立执行：
//   1. new_page 创建10页并写入 page_no 作为数据
//   2. unpin_page 解锁 → fetch_page 取回并校验数据一致性
//   3. delete_page 删除页面
//   4. flush_all_pages 刷盘
// 验证多线程并发下不会出现数据错乱、死锁或空指针。
TEST_F(BufferPoolManagerConcurrencyTest, ConcurrencyTest) {
    const int num_threads = 5;
    const int num_runs = 50;

    int fd = BufferPoolManagerConcurrencyTest::fd_;

    for (int run = 0; run < num_runs; run++) {
        auto bpm = std::make_shared<BufferPoolManager>();

        std::vector<std::thread> threads;
        for (int tid = 0; tid < num_threads; tid++) {
            threads.push_back(std::thread([&bpm, fd]() {  // NOLINT
                // 1. 创建10页，将 page_no 写入页内数据
                PageId temp_page_id = {.fd = fd, .page_no = INVALID_PAGE_ID};
                std::vector<PageId> page_ids;
                for (int i = 0; i < 10; i++) {
                    auto new_page = bpm->new_page(&temp_page_id);
                    EXPECT_NE(nullptr, new_page);
                    ASSERT_NE(nullptr, new_page);
                    strcpy(new_page->get_data(), std::to_string(temp_page_id.page_no).c_str());  // NOLINT
                    page_ids.push_back(temp_page_id);
                }
                // 2. 解锁所有页
                for (int i = 0; i < 10; i++) {
                    EXPECT_EQ(1, bpm->unpin_page(page_ids[i], true));
                }
                // 3. 重新取回并校验数据未被破坏
                for (int j = 0; j < 10; j++) {
                    auto page = bpm->fetch_page(page_ids[j]);
                    EXPECT_NE(nullptr, page);
                    ASSERT_NE(nullptr, page);
                    EXPECT_EQ(0, std::strcmp(std::to_string(page_ids[j].page_no).c_str(), (page->get_data())));
                    EXPECT_EQ(1, bpm->unpin_page(page_ids[j], true));
                }
                // 4. 删除所有页
                for (int j = 0; j < 10; j++) {
                    EXPECT_EQ(1, bpm->delete_page(page_ids[j]));
                }
                bpm->flush_all_pages(fd);
            }));
        }

        for (int i = 0; i < num_threads; i++) {
            threads[i].join();
        }
    }
}

// 存储层综合测试，覆盖 DiskManager 和 BufferPoolManager：
//   1. DiskManager: 文件的创建/打开/关闭/销毁，异常路径（重复创建、打开不存在文件）
//   2. BufferPoolManager: new_page → 写入随机数据 → unpin → check_cache 校验
//   3. flush_all_pages / flush_page 刷盘后 check_disk 校验磁盘数据一致
//   4. 随机混合操作（fetch → modify → unpin → flush → re-open）10000 轮压力测试
TEST(StorageTest, SimpleTest) {
    srand((unsigned)time(nullptr));

    // ===== 第1阶段：测试 DiskManager 文件操作 =====
    std::vector<std::string> filenames(MAX_FILES);  // MAX_FILES=32
    std::unordered_map<int, std::string> fd2name;
    for (size_t i = 0; i < filenames.size(); i++) {
        auto &filename = filenames[i];
        filename = std::to_string(i) + ".txt";
        if (disk_manager.is_file(filename)) {
            disk_manager.destroy_file(filename);
        }
        // open without create
        try {
            disk_manager.open_file(filename);
            assert(false);
        } catch (const FileNotFoundError &e) {
        }

        disk_manager.create_file(filename);
        assert(disk_manager.is_file(filename));
        try {
            disk_manager.create_file(filename);
            assert(false);
        } catch (const FileExistsError &e) {
        }

        // open file
        int fd = disk_manager.open_file(filename);
        char *tmp = new char[PAGE_SIZE * MAX_PAGES];  // TODO: fix error in detected memory leaks

        mock[fd] = tmp;
        fd2name[fd] = filename;

        disk_manager.set_fd2pageno(fd, 0);  // diskmanager在fd对应的文件中从0开始分配page_no
    }

    // ===== 第2阶段：测试 BufferPoolManager 页面分配 =====
    int num_pages = 0;
    char init_buf[PAGE_SIZE];
    for (auto &fh : mock) {
        int fd = fh.first;
        for (page_id_t i = 0; i < MAX_PAGES; i++) {
            rand_buf(PAGE_SIZE, init_buf);  // 将init_buf填充PAGE_SIZE个字节的随机数据

            PageId tmp_page_id = {.fd = fd, .page_no = INVALID_PAGE_ID};
            Page *page = buffer_pool_manager.new_page(&tmp_page_id);
            int page_no = tmp_page_id.page_no;
            assert(page_no != INVALID_PAGE_ID);
            assert(page_no == i);

            memcpy(page->get_data(), init_buf, PAGE_SIZE);
            buffer_pool_manager.unpin_page(PageId{fd, page_no}, true);

            char *mock_buf = mock_get_page(fd, page_no);  // &mock[fd][page_no * PAGE_SIZE]
            memcpy(mock_buf, init_buf, PAGE_SIZE);

            num_pages++;

            check_cache(fd, page_no);  // 调用了fetch_page, unpin_page
        }
    }
    check_cache_all();

    assert(num_pages == TEST_BUFFER_POOL_SIZE);

    // ===== 第3阶段：测试 flush_all_pages() 刷盘 =====
    for (auto &entry : fd2name) {
        int fd = entry.first;
        buffer_pool_manager.flush_all_pages(fd);
        for (int page_no = 0; page_no < MAX_PAGES; page_no++) {
            check_disk(fd, page_no);
        }
    }
    check_disk_all();

    // ===== 第4阶段：随机混合操作压力测试（10000 轮）=====
    for (int r = 0; r < 10000; r++) {
        int fd = rand_fd();
        int page_no = rand() % MAX_PAGES;
        Page *page = buffer_pool_manager.fetch_page(PageId{fd, page_no});
        char *mock_buf = mock_get_page(fd, page_no);
        assert(memcmp(page->get_data(), mock_buf, PAGE_SIZE) == 0);

        // modify
        rand_buf(PAGE_SIZE, init_buf);
        memcpy(page->get_data(), init_buf, PAGE_SIZE);
        memcpy(mock_buf, init_buf, PAGE_SIZE);

        buffer_pool_manager.unpin_page(page->get_page_id(), true);
        // BufferPool::mark_dirty(page);

        // flush
        if (rand() % 10 == 0) {
            buffer_pool_manager.flush_page(page->get_page_id());
            check_disk(fd, page_no);
        }
        // flush entire file
        if (rand() % 100 == 0) {
            buffer_pool_manager.flush_all_pages(fd);
        }
        // re-open file
        if (rand() % 100 == 0) {
            disk_manager.close_file(fd);
            auto filename = fd2name[fd];
            char *buf = mock[fd];
            fd2name.erase(fd);
            mock.erase(fd);
            int new_fd = disk_manager.open_file(filename);
            mock[new_fd] = buf;
            fd2name[new_fd] = filename;
        }
        // assert equal in cache
        check_cache(fd, page_no);
    }
    check_cache_all();

    for (auto &entry : fd2name) {
        int fd = entry.first;
        buffer_pool_manager.flush_all_pages(fd);
        for (int page_no = 0; page_no < MAX_PAGES; page_no++) {
            check_disk(fd, page_no);
        }
    }
    check_disk_all();

    // close and destroy files
    for (auto &entry : fd2name) {
        int fd = entry.first;
        auto &filename = entry.second;
        buffer_pool_manager.delete_all_pages(fd);  // 清理缓冲池残留，防止 fd 复用污染
        disk_manager.close_file(fd);
        disk_manager.destroy_file(filename);
        try {
            disk_manager.destroy_file(filename);
            assert(false);
        } catch (const FileNotFoundError &e) {
        }
    }
}

// 记录管理器综合测试，覆盖 RmManager 和 RmFileHandle：
//   1. 文件级操作：create_file / open_file / close_file / destroy_file
//   2. 文件头持久化：修改 num_pages 后 close 再 reopen，验证数据正确回读
//   3. 记录 CRUD：1000 轮随机 insert / update / delete，通过内存 mock 交叉校验
//   4. 记录扫描：通过 RmScan 遍历全部记录，验证与 mock 一致
//   5. 定期 re-open：每50轮关闭再打开文件，验证持久化一致性
TEST(RecordManagerTest, SimpleTest) {
    srand((unsigned)time(nullptr));

    auto rm_manager = std::make_unique<RmManager>();

    std::unordered_map<Rid, std::string, rid_hash_t, rid_equal_t> mock;

    std::string filename = "abc.txt";

    int record_size = 4 + rand() % 256;
    // ===== 第1阶段：测试文件创建与文件头持久化 =====
    {
        // 删除残留的同名文件
        if (disk_manager.is_file(filename)) {
            disk_manager.destroy_file(filename);
        }
        // 将file header写入到磁盘中的filename文件
        rm_manager->create_file(filename, record_size);
        // 将磁盘中的filename文件读出到内存中的file handle的file header
        std::unique_ptr<RmFileHandle> file_handle = rm_manager->open_file(filename);
        // 检查filename文件在内存中的file header的参数
        assert(file_handle->file_hdr_.record_size == record_size);
        assert(file_handle->file_hdr_.first_free_page_no == RM_NO_PAGE);
        assert(file_handle->file_hdr_.num_pages == 1);

        int max_bytes = file_handle->file_hdr_.record_size * file_handle->file_hdr_.num_records_per_page +
                        file_handle->file_hdr_.bitmap_size + (int)sizeof(RmPageHdr);
        assert(max_bytes <= PAGE_SIZE);
        int rand_val = rand();
        file_handle->file_hdr_.num_pages = rand_val;
        rm_manager->close_file_and_clear_buffer(file_handle.get());

        // reopen file
        file_handle = rm_manager->open_file(filename);
        assert(file_handle->file_hdr_.num_pages == rand_val);
        rm_manager->close_file_and_clear_buffer(file_handle.get());
        rm_manager->destroy_file(filename);
    }
    // ===== 第2阶段：测试记录 CRUD 与扫描 =====
    rm_manager->create_file(filename, record_size);
    auto file_handle = rm_manager->open_file(filename);

    char write_buf[PAGE_SIZE];
    size_t add_cnt = 0;
    size_t upd_cnt = 0;
    size_t del_cnt = 0;
    for (int round = 0; round < 1000; round++) {
        float insert_prob = 1. - mock.size() / 250.;
        float dice = rand() * 1. / RAND_MAX;
        if (mock.empty() || dice < insert_prob) {
            rand_buf(file_handle->file_hdr_.record_size, write_buf);
            TupleMeta meta{0, false};
            Rid rid = file_handle->insert_record(meta, write_buf);
            mock[rid] = std::string((char *)write_buf, file_handle->file_hdr_.record_size);
            add_cnt++;
            //            std::cout << "insert " << rid << '\n'; // operator<<(cout,rid)
        } else {
            // update or erase random rid
            int rid_idx = rand() % mock.size();
            auto it = mock.begin();
            for (int i = 0; i < rid_idx; i++) {
                it++;
            }
            auto rid = it->first;
            if (rand() % 2 == 0) {
                // update
                rand_buf(file_handle->file_hdr_.record_size, write_buf);
                TupleMeta meta{0, false};
                file_handle->update_record(rid, meta, write_buf);
                mock[rid] = std::string((char *)write_buf, file_handle->file_hdr_.record_size);
                upd_cnt++;
                //                std::cout << "update " << rid << '\n';
            } else {
                // erase
                file_handle->delete_record(rid);
                mock.erase(rid);
                del_cnt++;
                //                std::cout << "delete " << rid << '\n';
            }
        }
        // Randomly re-open file
        if (round % 50 == 0) {
            rm_manager->close_file_and_clear_buffer(file_handle.get());
            file_handle = rm_manager->open_file(filename);
        }
        check_equal(file_handle.get(), mock);
    }
    assert(mock.size() == add_cnt - del_cnt);
    std::cout << "insert " << add_cnt << '\n' << "delete " << del_cnt << '\n' << "update " << upd_cnt << '\n';
    // clean up
    rm_manager->close_file_and_clear_buffer(file_handle.get());
    rm_manager->destroy_file(filename);
}
