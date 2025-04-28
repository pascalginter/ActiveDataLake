#include "DataController.hpp"
#include "MetadataController.hpp"
#include "IcebergCatalogController.hpp"

thread_local pqxx::connection DataController::conn =
    pqxx::connection{"postgresql://pascal-ginter@localhost/iceberg"};
thread_local pqxx::connection IcebergMetadataController::conn =
    pqxx::connection{"postgresql://pascal-ginter@localhost/iceberg"};
thread_local pqxx::connection IcebergCatalogController::conn =
    pqxx::connection{"postgresql://pascal-ginter@localhost/iceberg"};
thread_local pqxx::connection EvictionJob::conn =
    pqxx::connection{"postgresql://pascal-ginter@localhost/iceberg"};
int EvictionJob::counter = 0;