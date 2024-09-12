#include "AppComponent.hpp"

#include "controller/StaticController.hpp"
#include "controller/Iceberg/IcebergCatalogController.hpp"

#include "oatpp/network/Server.hpp"

#include <btrblocks/scheme/SchemePool.hpp>

#include <iostream>

void run() {
    AppComponent components; // Create scope Environment components

    /* Get router component */
    OATPP_COMPONENT(std::shared_ptr<oatpp::web::server::HttpRouter>, router);

    router->addController(StaticController::createShared());
    router->addController(IcebergCatalogController::createShared());

    /* Get connection handler component */
    OATPP_COMPONENT(std::shared_ptr<oatpp::network::ConnectionHandler>, connectionHandler);

    /* Get connection provider component */
    OATPP_COMPONENT(std::shared_ptr<oatpp::network::ServerConnectionProvider>, connectionProvider);

    /* create server */
    oatpp::network::Server server(connectionProvider,
                                  connectionHandler);

    OATPP_LOGd("Server", "Running on port {}...", connectionProvider->getProperty("port").toString())

    server.run();
}

/**
 *  main
 */
int main(int argc, const char * argv[]) {

    btrblocks::SchemePool::refresh();

    oatpp::Environment::init();

    run();

    /* Print how many objects were created during app running, and what have left-probably leaked */
    /* Disable object counting for release builds using '-D OATPP_DISABLE_ENV_OBJECT_COUNTERS' flag for better performance */
    std::cout << "\nEnvironment:\n";
    std::cout << "objectsCount = " << oatpp::Environment::getObjectsCount() << "\n";
    std::cout << "objectsCreated = " << oatpp::Environment::getObjectsCreated() << "\n\n";

    oatpp::Environment::destroy();

    return 0;
}
