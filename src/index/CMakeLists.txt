set(SOURCES ix_index_handle.cpp ix_scan.cpp ix_scan_temp.cpp)
add_library(index STATIC ${SOURCES})
target_link_libraries(index storage)