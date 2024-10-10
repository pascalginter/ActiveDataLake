#include <iostream>

#include "LocalFile.hpp"
#include "MemoryBufferedTransformedFile.hpp"
#include "LazilyTransformedFile.hpp"

#define LAZY_COMPUTATION true

std::shared_ptr<VirtualizedFile> VirtualizedFile::createFileAbstraction(std::string tableName) {
    std::string localPathPrefix = "../data/";
    if (std::filesystem::exists(localPathPrefix + tableName + ".parquet")) {
        return std::make_shared<LocalFile>(localPathPrefix + tableName + ".parquet");
    }else if (std::filesystem::exists(localPathPrefix + tableName)){
        if (LAZY_COMPUTATION){
            return std::make_shared<LazilyTransformedFile>(localPathPrefix + tableName);
        }else{
            return std::make_shared<MemoryBufferedTransformedFile>(localPathPrefix + tableName);
        }

    }else{
        std::cout << "file not found " << tableName << std::endl;
    }
}