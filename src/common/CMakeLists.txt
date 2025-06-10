set(COMMON_SOURCES TraceStack.cpp)
add_library(common STATIC ${COMMON_SOURCES})
target_include_directories(common PUBLIC ${PROJECT_SOURCE_DIR}/src)