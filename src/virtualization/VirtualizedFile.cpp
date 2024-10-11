#include <iostream>

#include "LocalFile.hpp"
#include "MemoryBufferedTransformedFile.hpp"
#include "LazilyTransformedFile.hpp"

#define LAZY_COMPUTATION true

std::shared_ptr<VirtualizedFile> VirtualizedFile::createFileAbstraction(std::string tableName) {
    std::string localPathPrefix = "../data/";
    if (std::filesystem::exists(localPathPrefix + tableName + ".parquet")) {
        return std::make_shared<LocalFile>(localPathPrefix + tableName + ".parquet");
    }
    if (std::filesystem::exists(localPathPrefix + tableName)){
        std::cout << localPathPrefix + tableName + "/metadata" << std::endl;
        assert(std::filesystem::exists(localPathPrefix + tableName + "/metadata"));
        if (LAZY_COMPUTATION){
            return std::make_shared<LazilyTransformedFile>(localPathPrefix + tableName);
        }else{
            return std::make_shared<MemoryBufferedTransformedFile>(localPathPrefix + tableName);
        }
    }
    std::cout << "file not found " << tableName << std::endl;
    return nullptr;

}