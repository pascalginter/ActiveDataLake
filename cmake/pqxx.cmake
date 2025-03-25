# ---------------------------------------------------------------------------
# pqxx
# ---------------------------------------------------------------------------

include(ExternalProject)
find_package(Git REQUIRED)

# Get pqxx
ExternalProject_Add(
        pqxx_src
        PREFIX "vendor/pqxx"
        GIT_REPOSITORY "https://github.com/jtv/libpqxx.git"
        GIT_TAG 7.10.0
        TIMEOUT 10
        CMAKE_ARGS
        -DBUILD_SHARED_LIBS=on
        -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/vendor/pqxx
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
        UPDATE_COMMAND ""
)

# Prepare pqxx
ExternalProject_Get_Property(pqxx_src install_dir)
set(PQXX_INCLUDE_DIR ${install_dir}/include)
set(PQXX_LIBRARY_PATH ${install_dir}/lib/libpqxx.so)
file(MAKE_DIRECTORY ${PQXX_INCLUDE_DIR})
include_directories(${PQXX_INCLUDE_DIR})
add_library(pqxx SHARED IMPORTED)
set_property(TARGET pqxx PROPERTY IMPORTED_LOCATION ${PQXX_LIBRARY_PATH})
set_property(TARGET pqxx APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${PQXX_INCLUDE_DIR})

# Dependencies
add_dependencies(pqxx pqxx_src)
