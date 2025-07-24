#pragma once

#ifndef TO_CHAR_H_
#define TO_CHAR_H_

#include <string>

namespace util {

static thread_local char buffer[64];


template <typename T>
const char* to_char(const T &value) {
    return std::to_string(value).c_str();
}

template <>
const char* to_char<char>(const char &value) {
    buffer[0] = value;
    buffer[1] = '\0';
    return buffer;
}

template <>
const char* to_char<unsigned char>(const unsigned char &value) {
    buffer[0] = value;
    buffer[1] = '\0';
    return buffer;
}

template <>
const char* to_char<int>(const int &value) {
    snprintf(buffer, sizeof(buffer), "%d", value);
    return buffer;
}

template <>
const char* to_char<unsigned int>(const unsigned &value) {
    snprintf(buffer, sizeof(buffer), "%u", value);
    return buffer;
}

template <>
const char* to_char<long>(const long &value) {
    snprintf(buffer, sizeof(buffer), "%ld", value);
    return buffer;
}

template <>
const char* to_char<unsigned long>(const unsigned long &value) {
    snprintf(buffer, sizeof(buffer), "%lu", value);
    return buffer;
}

template <>
const char* to_char<long long>(const long long &value) {
    snprintf(buffer, sizeof(buffer), "%lld", value);
    return buffer;
}

template <>
const char* to_char<unsigned long long>(const unsigned long long &value) {
    snprintf(buffer, sizeof(buffer), "%llu", value);
    return buffer;
}

template <>
const char* to_char<float>(const float &value) {
    snprintf(buffer, sizeof(buffer), "%.6f", value);
    return buffer;
}

template <>
const char* to_char<double>(const double &value) {
    snprintf(buffer, sizeof(buffer), "%.6lf", value);
    return buffer;
}


}



#endif