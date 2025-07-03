set(SOURCES 
        disk_manager.cpp 
        buffer_pool_manager.cpp
        buffer_pool_instance.cpp
        ../replacer/replacer.h 
        ../replacer/lru_replacer.cpp
        ../replacer/sharded_lru_replacer.cpp
)
add_library(storage STATIC ${SOURCES})
