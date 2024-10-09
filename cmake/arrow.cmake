set(ARROW_PREFIX ${CMAKE_BINARY_DIR}/vendor/btrblocks/src/btrblocks_src-build/vendor/arrow)
include_directories(${ARROW_PREFIX}/include)
link_directories(${ARROW_PREFIX}/lib)
