cmake_minimum_required(VERSION 3.16)
project(Db2)

set(CMAKE_CXX_STANDARD 14)
SET(CMAKE_CXX_FLAGS  "-Wall -Wextra -Weffc++ -Werror")

find_package(Boost 1.55 REQUIRED system serialization)

include_directories(.)
include_directories(compression)
include_directories(core)
include_directories(doc)

include_directories(${Boost_INCLUDE_DIR})

add_executable(Db2
        compression/dictionary_compressed_column.hpp
        core/base_column.hpp
        core/column.hpp
        core/column_base_typed.hpp
        core/compressed_column.hpp
        core/global_definitions.hpp
        core/lookup_array.hpp
        doc/documentation.hpp
        base_column.cpp
        main.cpp
        unittest.hpp compression/bitvector_compressed_column.h)

target_link_libraries(Db2
        ${Boost_LIBRARIES})