CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PROJECT(xredis)

IF(APPLE)
ELSEIF(UNIX)
    SET(CMAKE_C_COMPILER "/opt/rh/devtoolset-2/root/usr/bin/gcc")
    SET(CMAKE_CXX_COMPILER "/opt/rh/devtoolset-2/root/usr/bin/c++")
ENDIF()

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -g -fPIC -Wall -D_REENTRANT -DUSE_APACHE -DNO_STRING_CIPHER ")
SET(TARGET "xredis")
SET(LIBRARY_OUTPUT_PATH ../lib/release_linux/libRedis)

AUX_SOURCE_DIRECTORY(src SOURCE_DIRS_SRC)

INCLUDE_DIRECTORIES(src)
INCLUDE_DIRECTORIES(../include)

#add_executable(BaseSocket ${SOURCE_DIRS})

ADD_LIBRARY(${TARGET} STATIC ${SOURCE_DIRS_SRC})

TARGET_LINK_LIBRARIES(${TARGET} -lhiredis -std=c++11 -lnsl -lc -lm -lpthread -lstdc++)