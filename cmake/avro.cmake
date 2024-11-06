# ---------------------------------------------------------------------------
# avro
# ---------------------------------------------------------------------------

include(ExternalProject)
find_package(Git REQUIRED)

# Get avro
ExternalProject_Add(
        avro_src
        PREFIX "vendor/avro"
        GIT_REPOSITORY "https://github.com/apache/avro.git"
        SOURCE_SUBDIR lang/c++
        GIT_TAG main
        TIMEOUT 10
        CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/vendor/avro
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
        UPDATE_COMMAND ""
)

# Prepare avro
ExternalProject_Get_Property(avro_src install_dir)
set(AVRO_INCLUDE_DIR ${install_dir}/include)
set(AVRO_LIBRARY_PATH ${install_dir}/lib/libavrocpp.so)
file(MAKE_DIRECTORY ${AVRO_INCLUDE_DIR})
add_library(avro SHARED IMPORTED)
set_property(TARGET avro PROPERTY IMPORTED_LOCATION ${AVRO_LIBRARY_PATH})
set_property(TARGET avro APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${AVRO_INCLUDE_DIR})

# Dependencies
add_dependencies(avro avro_src)
