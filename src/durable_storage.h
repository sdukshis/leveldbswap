#ifndef DURABLE_STORAGE_H
#define DURABLE_STORAGE_H

#include <string>
#include <unordered_map>
#include <stdexcept>

#include <leveldb/db.h>

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


 private:
    void flush_cache();
    
    bool in_cache(const std::string &key, std::string &value);
    
    void update_cache(const std::string &key, const std::string &value);

    using Cache = std::unordered_map<std::string, std::string>;

    std::string dbpath_;
    std::size_t max_cache_size_;
    Cache cache_;
    leveldb::DB *db_;
    mutable std::size_t cache_size_;
};


DurableStorage::DurableStorage(const std::string &dbpath,
                               std::size_t cache_size) :
    dbpath_(dbpath), max_cache_size_(cache_size),
    db_(nullptr)
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
    
    if (!in_cache(key, value)) {
        // LOG("Cache miss: " << key);
        leveldb::Status s = db_->Get(leveldb::ReadOptions(), key, &value);
        if (!s.ok()) throw std::runtime_error("DurableStorage::get: " + s.ToString());
        update_cache(key, value);
    }
}

void DurableStorage::put(const std::string &key, const std::string &value)
{
    update_cache(key, value);
}

void DurableStorage::erase(const std::string &key)
{
    cache_.erase(key);
    leveldb::Status s = db_->Delete(leveldb::WriteOptions(), key);
    if (!s.ok()) throw std::runtime_error("DurableStorage::erase: " + s.ToString());
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
    cache_size_ += key.size() + value.size();
    flush_cache();
}

void DurableStorage::flush_cache()
{
    if (cache_size_ >= max_cache_size_) {
        auto it = cache_.begin();
        while (it != cache_.end() && 
               cache_size_ >= max_cache_size_ * 0.8) {
            leveldb::Status s = db_->Put(leveldb::WriteOptions(), it->first, it->second);
            if (!s.ok()) throw std::runtime_error("DurableStorage::put: " + s.ToString());
            cache_size_ -= it->first.size() + it->second.size();
            ++it;
        }
        LOG("cache size: " << cache_size_);
        cache_.erase(cache_.begin(), it);
    }
}


#endif