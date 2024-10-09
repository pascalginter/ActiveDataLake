# ---------------------------------------------------------------------------
# oatpp
# ---------------------------------------------------------------------------

include(ExternalProject)
find_package(Git REQUIRED)

# Get oatpp
ExternalProject_Add(
        oatpp_src
        PREFIX "vendor/oatpp"
        GIT_REPOSITORY "https://github.com/oatpp/oatpp"
        UPDATE_COMMAND git pull
)

ExternalProject_Get_Property(oatpp_src install_dir)
set(OATPP_LIBRARY_PATH ${install_dir}/src/oatpp_src-build/src/liboatpp.a)
set(OATPP_INCLUDE_DIR ${install_dir}/src/oatpp_src/src)

add_library(oatpp SHARED IMPORTED)
set_property(TARGET oatpp PROPERTY IMPORTED_LOCATION ${OATPP_LIBRARY_PATH})
set_property(TARGET oatpp APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${OATPP_INCLUDE_DIR})

# Dependencies
add_dependencies(oatpp oatpp_src)