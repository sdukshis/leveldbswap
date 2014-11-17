#ifndef DURABLE_STORAGE_H
#define DURABLE_STORAGE_H

#include <string>
#include <unordered_map>
#include <map>
#include <stdexcept>

#include <leveldb/db.h>

#include "meminfo.h"

#define LOG(msg) std::clog << msg << std::endl;

class DurableStorage {
 public:
    explicit DurableStorage(const std::string &dbpath,
                            std::size_t cache_size);

    ~DurableStorage();

    DurableStorage(const DurableStorage &) = delete;

    void operator=(const DurableStorage &) = delete;

    void get(const std::string &key, std::string &value);

    void put(const std::string &key, const std::string &value);

    void erase(const std::string &key);

    void stats();

 private:
    void flush_cache();
    
    bool in_cache(const std::string &key, std::string &value);
    
    void update_cache(const std::string &key, const std::string &value);

    using Cache = std::map<std::string, std::string>;

    std::string dbpath_;
    std::size_t max_cache_size_;
    Cache cache_;
    leveldb::DB *db_;
    size_t cache_miss_;
    size_t puts_;
    size_t gets_;
    size_t dels_;
};


DurableStorage::DurableStorage(const std::string &dbpath,
                               std::size_t cache_size) :
    dbpath_(dbpath), max_cache_size_(cache_size),
    db_(nullptr), cache_miss_(0), puts_(0), gets_(0), dels_(0)
{
    leveldb::Options options;
    options.create_if_missing = true;
    // options.error_if_exists = true;
    leveldb::Status s = leveldb::DB::Open(options, dbpath_, &db_);
    if (!s.ok()) throw std::runtime_error(s.ToString());
}

DurableStorage::~DurableStorage()
{
    // flush_cache();
    delete db_;
}

void DurableStorage::get(const std::string &key, std::string &value)
{
    ++gets_; 
    if (!in_cache(key, value)) {
        ++cache_miss_;
        // LOG("Cache miss: " << key);
        leveldb::Status s = db_->Get(leveldb::ReadOptions(), key, &value);
        if (!s.ok()) return; //throw std::runtime_error("DurableStorage::get: " + s.ToString());
        update_cache(key, value);
    }
}

void DurableStorage::put(const std::string &key, const std::string &value)
{
    ++puts_;
    update_cache(key, value);
}

void DurableStorage::erase(const std::string &key)
{
    ++dels_;
    cache_.erase(key);
    leveldb::Status s = db_->Delete(leveldb::WriteOptions(), key);
    if (!s.ok()) throw std::runtime_error("DurableStorage::erase: " + s.ToString());
}

void DurableStorage::stats()
{
    std::cout << "cache_miss: " << cache_miss_
         << " puts_: " << puts_ 
         << " gets_: " << gets_ 
         << " dels_: " << dels_
         << std::endl;
    cache_miss_ = puts_ = gets_ = dels_ = 0; 
}

bool DurableStorage::in_cache(const std::string &key, std::string &value)
{
    auto it = cache_.find(key);
    if (it == cache_.end()) {
        return false;
    }
    value = it->second;
    return true;
}

void DurableStorage::update_cache(const std::string &key, const std::string &value)
{
    cache_[key] = value;
    flush_cache();
}

void DurableStorage::flush_cache()
{
    if (getCurrentRSS() >= max_cache_size_ * 0.95) {
        // LOG("Cleanup begin: " << getCurrentRSS());
        auto it = cache_.begin();
        while (it != cache_.end() && 
               getCurrentRSS() >= max_cache_size_ * 0.8) {
            leveldb::Status s = db_->Put(leveldb::WriteOptions(), it->first, it->second);
            if (!s.ok()) throw std::runtime_error("DurableStorage::put: " + s.ToString());
            cache_.erase(it++);
        }
        // if (cache_.empty()) LOG("Cache cleared");
        // LOG("Cleanup end:   " << getCurrentRSS());
    }
}


#endif