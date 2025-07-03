set(SOURCES lru_replacer.cpp sharded_lru_replacer.cpp)
add_library(lru_replacer STATIC ${SOURCES})
add_library(shared_lru_replacer STATIC ${SOURCES})