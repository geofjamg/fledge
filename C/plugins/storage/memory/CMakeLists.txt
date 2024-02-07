cmake_minimum_required(VERSION 2.6.0)

project(memory)

set(CMAKE_CXX_FLAGS_DEBUG "-O0 -ggdb")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
set(STORAGE_COMMON_LIB storage-common-lib)

# Add sqlitememory files
file(GLOB SOURCES *.cpp)

# Include header files
include_directories(../../../common/include)
include_directories(../../../services/common/include)
include_directories(../common/include)
include_directories(../../../thirdparty/rapidjson/include)

link_directories(${PROJECT_BINARY_DIR}/../../../lib)

# Create shared library
add_library(${PROJECT_NAME} SHARED ${SOURCES} ${COMMON_SOURCES})
set_target_properties(${PROJECT_NAME} PROPERTIES SOVERSION 1)
target_link_libraries(${PROJECT_NAME} ${STORAGE_COMMON_LIB})

add_definitions(-DMEMORY_READING_PLUGIN=1)

# Install library
install(TARGETS ${PROJECT_NAME} DESTINATION fledge/plugins/storage/${PROJECT_NAME})
