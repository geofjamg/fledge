#ifndef _MEMORY_CONTEXT_H
#define _MEMORY_CONTEXT_H

#include <condition_variable>
#include "rapidjson/document.h"
#include <mutex>
#include <thread>
#include <vector>

// C++11 does not have a read write lock and boost shared_mutex does not work
class ReaderWriterLock {
    std::mutex mtx;
    std::condition_variable cv;
    int readers = 0;
    bool writer = false;

public:
    void lockRead() {
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait(lock, [this](){ return !writer; });
        ++readers;
    }

    void unlockRead() {
        std::unique_lock<std::mutex> lock(mtx);
        if (--readers == 0) {
            cv.notify_one();
        }
    }

    void lockWrite() {
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait(lock, [this](){ return !writer && readers == 0; });
        writer = true;
    }

    void unlockWrite() {
        std::unique_lock<std::mutex> lock(mtx);
        writer = false;
        cv.notify_all();
    }
};

struct Reading {
    std::string _assetCode;
    std::string _userTs;
    std::string _ts;
    std::string _json;

    Reading(std::string assetCode, std::string userTs, std::string ts, std::string json)
        : _assetCode(std::move(assetCode)),
          _userTs(std::move(userTs)),
          _ts(std::move(ts)),
          _json(std::move(json)) {
    }
};

class MemoryContext {
public:
    MemoryContext()
        : _readingMinId(0) {
    }

    int addReading(const char* readings);
    void purgeReadingsByRow(unsigned long maxRows, unsigned long sent, unsigned long& removed, unsigned long& unsentPurged, unsigned long& unsentRetained,
                            unsigned long& readings, unsigned int& duration);
    void purgeReadingsByAge(unsigned long maxAge, unsigned long sent, unsigned long& removed, unsigned long& unsentPurged, unsigned long& unsentRetained,
                            unsigned long& readings, unsigned int& duration);
    char* fetchReadings(unsigned long firstId, unsigned int blkSize);
    unsigned int getReadingCount() const { return _readings.size(); }

private:
    ReaderWriterLock rwLock;
    unsigned long _readingMinId;
    std::vector<Reading> _readings;
    rapidjson::Document _document;
};

#endif
