set(SOURCES lru_replacer.cpp clock_replacer.cpp)
add_library(lru_replacer STATIC ${SOURCES})
add_library(clock_replacer STATIC ${SOURCES})