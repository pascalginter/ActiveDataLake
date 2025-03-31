#ifndef DATA_CONTROLLER_HPP
#define DATA_CONTROLLER_HPP

#include <fstream>
#include <sys/mman.h>
#include <sys/stat.h>

#include <arrow/io/api.h>

#include <btrblocks/arrow/DirectoryReader.hpp>
#include <btrblocks.hpp>
#include <fmt/format.h>

#include "oatpp/web/server/api/ApiController.hpp"
#include "oatpp/json/ObjectMapper.hpp"
#include "oatpp/macro/codegen.hpp"
#include "oatpp/macro/component.hpp"



#include "pqxx/pqxx"

#include OATPP_CODEGEN_BEGIN(ApiController) //<- Begin Codegen

#include "oatpp/json/Serializer.hpp"

#include "../../virtualization/VirtualizedFile.hpp"
#include "../../virtualization/PostgresBufferedFile.hpp"

class DataController final : public oatpp::web::server::api::ApiController {
    static thread_local pqxx::connection conn;
    PostgresBufferedFile pbf;
public:
    explicit DataController(const std::shared_ptr<oatpp::web::mime::ContentMappers>& apiContentMappers)
        : oatpp::web::server::api::ApiController(apiContentMappers), pbf("") {
    }

private:
    oatpp::json::ObjectMapper objectMapper;
    std::unordered_map<std::string, std::shared_ptr<VirtualizedFile>> tableFiles;

public:

    static std::shared_ptr<DataController> createShared(
            OATPP_COMPONENT(std::shared_ptr<oatpp::web::mime::ContentMappers>, apiContentMappers) // Inject ContentMappers
    ){
        return std::make_shared<DataController>(apiContentMappers);
    }

    ENDPOINT("HEAD", "/data/{fileName}", headData,
             REQUEST(std::shared_ptr<IncomingRequest>, request),
             PATH(String, fileName)){
        const std::string_view fileNameView(*fileName);
        if (!fileNameView.ends_with(".parquet")){
            return createResponse(Status::CODE_501, "File format is not supported");
        }
        std::string tableName(fileNameView.begin(), fileName->size() - 8);
        if (tableFiles.find(tableName) == tableFiles.end()){
            tableFiles[tableName] = VirtualizedFile::createFileAbstraction(tableName);
        }
        auto& file = tableFiles[tableName];
        if (file == nullptr) {
            return createResponse(Status::CODE_404, "Not found");
        }
        auto response = createResponse(Status::CODE_200, "");
        S3InterfaceUtils::putByteSizeHeader(response, file->size());
        return response;
    }

    ENDPOINT("GET", "/data/{fileName}", getData,
             REQUEST(std::shared_ptr<IncomingRequest>, request),
             PATH(String, fileName)){
        const std::string_view fileNameView(*fileName);
        if (!fileNameView.ends_with(".parquet")){
            return createResponse(Status::CODE_501, "File format is not supported");
        }
        const std::string tableName(fileNameView.begin(), fileName->size() - 8);
        if (!tableFiles.contains(tableName)){
            tableFiles[tableName] = VirtualizedFile::createFileAbstraction(tableName);
        }

        auto range = S3InterfaceUtils::extractRange(request, tableFiles[tableName]->size());
        auto response = createResponse(Status::CODE_200, tableFiles[tableName]->getRange(range));
        S3InterfaceUtils::putByteSizeHeader(response, tableFiles[tableName]->size());
        return response;
    }

    ENDPOINT("HEAD", "/data/{tableName}/{fileName}", headNewData,
            REQUEST(std::shared_ptr<IncomingRequest>, request),
            PATH(String, tableName),
            PATH(String, fileName)) {
        std::cout << "beginning of time" << std::endl;
        std::cout << fileName.getValue("") << std::endl;
        if (fileName == "buffered.parquet") {
            auto response = createResponse(Status::CODE_200, "");
            S3InterfaceUtils::putByteSizeHeader(response, pbf.size());
            return response;
        }
        return createResponse(Status::CODE_404, "");
    }

    ENDPOINT("DELETE", "/data/{tableName}/{fileName}", deleteData,
            REQUEST(std::shared_ptr<IncomingRequest>, request),
            PATH(String, tableName),
            PATH(String, fileName)) {
        std::cout << "delete request" << std::endl;
        std::cout << "------------------------------------------------" << std::endl;
        return createResponse(Status::CODE_200, "");
    }

    ENDPOINT("PUT", "/data/{tableName}/{fileName}", putData,
    REQUEST(std::shared_ptr<IncomingRequest>, request),
         PATH(String, tableName),
         PATH(String, fileName)) {
        pqxx::work tx{conn};
        std::cout << "put matched" << std::endl;
        auto parameters = request->getQueryParameters().getAll();
        const oatpp::data::share::StringKeyLabel label("partNumber");
        assert(parameters.contains(label));
        int partId;
        const auto [fst, snd] = parameters.equal_range(label);
        for (auto b = fst; b != snd; ++b) {
            partId = atoi(b->second.std_str().c_str());
        }
        try {
            const auto content = request->readBodyToString();
            pqxx::binarystring bin_str(content.get()->data(), content.get()->size());

            tx.exec("INSERT INTO BufferedData(file_id, part, content, size) VALUES ($1, $2, $3, $4)",
                pqxx::params{0, partId, bin_str, bin_str.size()});
            tx.commit();
            auto response = createResponse(Status::CODE_200, "");
            response->putHeader("ETag", "\"b54357faf0632cce46e942fa68356b38\"");

            return response;
        }catch (const std::exception& e) {
            std::cout << e.what() << std::endl;
            return createResponse(Status::CODE_500, "");
        }
    }

    ENDPOINT("GET", "/data/{tableName}/{fileName}", getBufferedData,
            REQUEST(std::shared_ptr<IncomingRequest>, request),
            PATH(String, tableName),
            PATH(String, fileName)) {
        const auto range = S3InterfaceUtils::extractRange(request, pbf.size());
        std::cout << range.begin << " " << range.end << std::endl;
        const auto response = pbf.getRange(range);
        return createResponse(Status::CODE_200, response);
    }

    ENDPOINT("POST", "/data/{tableName}/{fileName}", postData,
             REQUEST(std::shared_ptr<IncomingRequest>, request),
             PATH(String, tableName),
             PATH(String, fileName)){
        std::cout << fileName->c_str() << std::endl;

        for (const auto& [key, value] : request->getQueryParameters().getAll()) {
            std::cout << key.std_str() << " | " << value.std_str() << std::endl;
        }

        //// Print headers
        for (const auto& [key, value] : request->getHeaders().getAll()) {
            std::cout << key.std_str() << " | " << value.std_str() << std::endl;
        }
        std::cout << "------------------------------------------------" << std::endl;


        std::string result = "<InitiateMultipartUploadResult>\n";
        result += "<Bucket>" + tableName + "</Bucket>\n";
        result += "<Key>" + fileName + "</Key>\n";
        result += "<UploadId>" + std::string("EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRe") + "</UploadId>\n";
        result += "</InitiateMultipartUploadResult>\n";

        auto response = createResponse(Status::CODE_200, result);
        return response;
    }
};

#include OATPP_CODEGEN_END(ApiController) //<- End Codegen

#endif //DATA_CONTROLLER_HPP