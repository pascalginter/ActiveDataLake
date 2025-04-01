#include <oatpp/async/Executor.hpp>
#include <pqxx/pqxx>

#include "../../virtualization/PostgresBufferedFile.hpp"

class EvictionJob : public oatpp::async::Coroutine<EvictionJob> {
    static thread_local pqxx::connection conn;
public:
    Action act() override {
        pqxx::work tx(conn);
        int threshold = 1024 * 1024 * 256;
        auto totalSize = tx.exec("SELECT SUM(size) FROM BufferedFiles WHERE finalized").one_row()[0].as<int>();
        std::cout << totalSize << " " << totalSize - threshold <<  std::endl
                  << "----------------------------" << std::endl;
        if (totalSize > threshold){
            std::cout << "big enough to evict" << std::endl;
            auto table = PostgresBufferedFile::getCombinedTable(tx);
            std::cout << table << std::endl;
            const auto out = arrow::io::FileOutputStream::Open("help.parquet").ValueOrDie();
            PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out));
            out->Close();
            std::cout << "wrote table" << std::endl;
            tx.exec("DELETE FROM BufferedData d WHERE EXISTS("
                        "SELECT * FROM BufferedFiles f WHERE d.file_id = f.file_id AND f.finalized)");
            tx.exec("DELETE FROM BufferedFiles WHERE finalized");
            std::cout << "Deleted successfully" << std::endl;
        }
        tx.commit();
        return finish();
    }
};


