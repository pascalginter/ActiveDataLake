#include <oatpp/async/Executor.hpp>
#include <pqxx/pqxx>

#include "../../virtualization/PostgresBufferedFile.hpp"

class EvictionJob final : public oatpp::async::Coroutine<EvictionJob> {
    //static thread_local pqxx::connection conn;
    //static int counter;
public:
    Action act() override {
        /*pqxx::work tx(conn);
        constexpr int threshold = 1024 * 1024 * 256;
        const auto totalSize = tx.exec("SELECT SUM(size) FROM BufferedFiles WHERE finalized").one_row()[0].as<int>();
        std::cout << totalSize << " " << totalSize - threshold <<  std::endl
                  << "----------------------------" << std::endl;
        if (totalSize > threshold){
            std::cout << "big enough to evict" << std::endl;
            const auto table = PostgresBufferedFile::getCombinedTable(tx);
            std::cout << table << std::endl;
            std::string fileName = "evicted-" + std::to_string(counter++) + ".parquet";
            const std::string filePath = "../data/" + fileName;
            const auto out = arrow::io::FileOutputStream::Open(filePath).ValueOrDie();
            PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out));
            std::cout << "file size" << out->Tell().ValueOrDie() << std::endl;
            out->Close();
            long fileSize = std::filesystem::file_size(filePath);;
            std::cout << "wrote table" << std::endl;
            tx.exec("DELETE FROM BufferedData d WHERE EXISTS("
                        "SELECT * FROM BufferedFiles f WHERE d.file_id = f.file_id AND f.finalized)");
            std::cout << "Deleted data" << std::endl;
            tx.exec("DELETE FROM BufferedFiles WHERE finalized");
            std::cout << "Deleted files" << std::endl;
            tx.exec("INSERT INTO ManifestEntry(file_name, record_count, file_size_in_bytes) "
                          "VALUES ($1, $2, $3)", pqxx::params(fileName, table->num_rows(), fileSize));
            std::cout << "Deleted successfully" << std::endl;
        }
        tx.commit();*/
        return finish();
    }
};


