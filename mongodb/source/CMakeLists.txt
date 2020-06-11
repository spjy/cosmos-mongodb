cmake_minimum_required(VERSION 2.8)
SET(PROJECT_NAME mongodb)
project(${PROJECT_NAME})

# point to cosmos/source folder
set(COSMOS_SOURCE ${PROJECT_SOURCE_DIR}/../../..)
set(USE_COSMOS_FROM "SOURCE")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -pedantic -std=c++17 -Wno-invalid-offsetof")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

# -----------------------------------------------
# To use COSMOS from the source folder
include(${COSMOS_SOURCE}/core/cmake/use_cosmos_from_source.cmake)

set(COSMOS_LIBS
    CosmosSupport
    CosmosAgent
    )

find_package(libmongocxx REQUIRED)
find_package(libbsoncxx REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Boost COMPONENTS system REQUIRED)

# -----------------------------------------------
# Windows specific
if(${CMAKE_SYSTEM_NAME} MATCHES "Windows")
    set(COSMOS_LIBS ${COSMOS_LIBS} ws2_32)
endif()

include_directories(${LIBMONGOCXX_INCLUDE_DIRS} ${LIBBSONCXX_INCLUDE_DIRS})
link_directories(${LIBMONGOCXX_INCLUDE_DIRS})
link_directories(${LIBBSONCXX_INCLUDE_DIRS})

include_directories(${PROJECT_SOURCE_DIR}/include)
message(${LIBBSONCXX_INCLUDE_DIRS})
add_executable(agent_mongo agent_mongo.cpp)
set_target_properties(agent_mongo PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/../../../../tools)

target_include_directories(agent_mongo
  PRIVATE ${LIBMONGOCXX_INCLUDE_DIRS} ${LIBBSONCXX_INCLUDE_DIRS} ${PROJECT_SOURCE_DIR}/include
  ${Boost_INCLUDE_DIR}
)

target_link_libraries(agent_mongo
  PRIVATE
  ${COSMOS_LIBS}
  ${LIBMONGOCXX_LIBRARIES}
  ${LIBBSONCXX_LIBRARIES}
  OpenSSL::SSL
  OpenSSL::Crypto
  ${Boost_SYSTEM_LIBRARY}
  stdc++fs
)

target_compile_definitions(agent_mongo
  PRIVATE ${LIBMONGOCXX_DEFINITIONS} ${LIBBSONCXX_DEFINITIONS}
)
