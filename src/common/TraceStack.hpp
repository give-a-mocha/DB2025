#pragma once

#ifndef TRACE_STACK_HPP_
#define TRACE_STACK_HPP_

#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "common/Format.h"
#include "common/print.hpp"

class TraceStackPrint {
   private:
    static std::vector<std::string> callStack;
    static int indentLevel;

    std::string funcName;
    std::string fileName;
    int lineNumber;

    static constexpr const char* ENTERMSG = "{}ENTER: {}({}:{})";
    static constexpr const char* EXITMSG = "{}EXIT: {}({}:{})";
    static constexpr const char* CallMSG = "======== Call Stack Trace ========";
    static constexpr const char* SPLITMSG = "==================================";

   public:
    TraceStackPrint(const std::string& func, const std::string& file, int line)
        : funcName(func), fileName(file), lineNumber(line) {
        std::string msg = util::format(ENTERMSG, std::string(indentLevel * 2, ' '), funcName, fileName, lineNumber);
        callStack.push_back(msg);
        INFO(msg);
        indentLevel++;
    }

    ~TraceStackPrint() {
        indentLevel--;
        std::string msg = util::format(EXITMSG, std::string(indentLevel * 2, ' '), funcName, fileName, lineNumber);

        callStack.push_back(msg);
        INFO(msg);
    }

    static void printStack() {
        INFO(CallMSG);
        for (const auto& entry : callStack) {
            INFO(entry);
        }
        INFO(SPLITMSG);
    }

    static void clearStack() {
        callStack.clear();
        indentLevel = 0;
    }
};
// #ifndef ENABLE_TRACE
// #define ENABLE_TRACE
// #endif
#ifndef ENABLE_TRACE
#undef ENABLE_TRACE
#endif
#ifdef ENABLE_TRACE
#define TRACE_FUNCTION TraceStackPrint tracer(__FUNCTION__, __FILE__, __LINE__);
#define TRACE_NAMED(name) TraceStackPrint tracer(name, __FILE__, __LINE__);
#define TRACE_PRINT_STACK TraceStackPrint::printStack();
#define TRACE_CLEAR_STACK TraceStackPrint::clearStack();
#else
#define TRACE_FUNCTION
#define TRACE_NAMED(name)
#define TRACE_PRINT_STACK
#define TRACE_CLEAR_STACK
#endif

#endif
