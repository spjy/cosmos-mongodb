cmake_minimum_required(VERSION 2.8)
project(mongodb)

#aux_source_directory(. SRC_LIST)
#add_executable(${PROJECT_NAME} ${SRC_LIST})

# point to cosmos/source folder
set(COSMOS_SOURCE   "../../../")
set(USE_COSMOS_FROM "SOURCE")

#set(CMAKE_MODULE_PATH "/usr/local/lib/")
#set(CMAKE_PREFIX_PATH, "/usr/local/lib/cmake/libbsoncxx-3.4.0" "usr/local/lib/cmake/libmongocxx-3.4.0")

#find_package(mongocxx-3.4.0 COMPONENTS)
#find_package(bsoncxx-3.4.0 COMPONENTS)

#include_directories("/Users/miguelnunes/")
include_directories("usr/include" "/usr/local/include/mongocxx/v_noabi" "/usr/local/include/bsoncxx/v_noabi" "${CMAKE_SOURCE_DIR}/../include" "/home/spjy/Simple-WebSocket-Server-master/" "/home/spjy/openssl-1.1.0j/include" "/home/spjy/boost_1_70_0/")
link_directories("/usr/local/lib")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -pedantic -std=c++11 -Wno-invalid-offsetof")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

# -----------------------------------------------
# To use COSMOS from the source folder
#include(${CMAKE_SOURCE_DIR}/use_cosmos_from_source.cmake)
include(${COSMOS_SOURCE}/core/cmake/use_cosmos_from_source.cmake)
#include("/usr/local/lib/cmake/libbsoncxx-3.4.0/libbsoncxx-config-version.cmake")
#include("/usr/local/lib/cmake/libmongocxx-3.4.0/libmongocxx-config-version.cmake")

#add_executable(agent_cpu "${COSMOS_SOURCE}/core/programs/agents/agent_cpu.cpp")
#target_link_libraries(agent_cpu
#    CosmosSupport
#    CosmosAgent
#    CosmosDeviceGeneral
#    CosmosDeviceCpu
#    CosmosDeviceDisk
#    CosmosMath
#    )

set(COSMOS_LIBS
    CosmosSupport
    CosmosAgent
    CosmosDeviceGeneral
    CosmosDeviceSerial
    CosmosDeviceCpu
    CosmosDeviceDisk
    )

# -----------------------------------------------
# Windows specific
if(${CMAKE_SYSTEM_NAME} MATCHES "Windows")
    set(COSMOS_LIBS ${COSMOS_LIBS} ws2_32)
endif()

add_executable(agent_mongo agent_mongo.cpp)

target_link_libraries(agent_mongo ${COSMOS_LIBS} bson-1.0 bsoncxx mongoc-1.0 mongocxx ssl crypto boost_system)

# -----------------------------------------------
# to deploy on a remote device
#file(WRITE "${CMAKE_SOURCE_DIR}/QtCreatorDeployment.txt" "cosmos/bin\n")

#macro(add_deployment_file SRC DEST)
#    file(RELATIVE_PATH path ${CMAKE_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR})
#    file(APPEND "${CMAKE_SOURCE_DIR}/QtCreatorDeployment.txt" "${path}/${SRC}:${DEST}\n")
#endmacro()

#macro(add_deployment_directory SRC DEST)
#    file(GLOB_RECURSE files RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" "${SRC}/*")
#    foreach(filename ${files})
#        get_filename_component(path ${filename} PATH)
#        add_deployment_file("${filename}" "${DEST}/${path}")
#    endforeach(filename)
#endmacro()

#set(program_name programs/agents/agent)
#file(RELATIVE_PATH relativeDir ${PROJECT_SOURCE_DIR} ${PROJECT_BINARY_DIR}/${program_name})
#add_deployment_file(${relativeDir} "bin")

#set(program_name "${COSMOS_SOURCE}/core/programs/agents/agent_cpu")
#file(RELATIVE_PATH relativeDir ${PROJECT_SOURCE_DIR} ${PROJECT_BINARY_DIR}/${program_name})
#add_deployment_file(${relativeDir} "bin")

