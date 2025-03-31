#include <iostream>

#include "LocalFile.hpp"
#include "MemoryBufferedTransformedFile.hpp"
#include "LazilyTransformedFile.hpp"
#include "RemoteFile.hpp"
#include "PrefetchFile.hpp"
#include "PostgresBufferedFile.hpp"

#define LAZY_COMPUTATION true
#define PREFETCH true

std::shared_ptr<VirtualizedFile> VirtualizedFile::createFileAbstraction(std::string tableName) {
    const std::string localPathPrefix = "../data/";
    if (std::filesystem::exists(localPathPrefix + tableName + ".parquet")) {
        return std::make_shared<LocalFile>(localPathPrefix + tableName + ".parquet");
    }
    if (std::filesystem::exists(localPathPrefix + tableName)){
        std::cout << localPathPrefix + tableName + "/metadata" << std::endl;
        assert(std::filesystem::exists(localPathPrefix + tableName + "/metadata"));
        if constexpr (LAZY_COMPUTATION){
            return std::make_shared<LazilyTransformedFile>(localPathPrefix + tableName);
        }else{
            return std::make_shared<MemoryBufferedTransformedFile>(localPathPrefix + tableName);
        }
    }
    return nullptr;
    if constexpr (PREFETCH) {
        return std::make_shared<PrefetchFile>(tableName);
    } else {
        return std::make_shared<RemoteFile>(tableName);
    }
    std::cout << "file not found " << tableName << std::endl;
    return nullptr;
}

thread_local std::string LazilyTransformedFile::buffer = "";
thread_local std::vector<uint8_t> LazilyTransformedFile::curr_buffer = {};

thread_local std::shared_ptr<std::string> LocalFile::result = std::make_shared<std::string>();

thread_local std::shared_ptr<std::string> RemoteFile::result = std::make_shared<std::string>();

thread_local std::shared_ptr<std::string> PostgresBufferedFile::result = std::make_shared<std::string>();
thread_local pqxx::connection PostgresBufferedFile::conn =
    pqxx::connection{"postgresql://pascal-ginter@localhost/iceberg"};;
std::shared_ptr<arrow::Buffer> PostgresBufferedFile::buffer = nullptr;

std::shared_ptr<std::string> PrefetchFile::result = std::make_shared<std::string>("");