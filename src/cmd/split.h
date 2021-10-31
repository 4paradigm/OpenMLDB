// Copyright 2008 and onwards Google, Inc.
//
// #status: RECOMMENDED
// #category: operations on strings
// #summary: Functions for splitting strings into substrings.
//
// This file contains functions for splitting strings. The new and recommended
// API for string splitting is the strings::Split() function. The old API is a
// large collection of standalone functions declared at the bottom of this file
// in the global scope.
//
// TODO(user): Rough migration plan from old API to new API
// (1) Add comments to old Split*() functions showing how to do the same things
//     with the new API.
// (2) Reimplement some of the old Split*() functions in terms of the new
//     Split() API. This will allow deletion of code in split.cc.
// (3) (Optional) Replace old Split*() API calls at call sites with calls to new
//     Split() API.
//

#ifndef SRC_CMD_SPLIT_H_
#define SRC_CMD_SPLIT_H_

#include <string>
#include <vector>
#include "absl/strings/ascii.h"

namespace openmldb::cmd {
// ----------------------------------------------------------------------
// strdup_with_new()
// strndup_with_new()
//
//    strdup_with_new() is the same as strdup() except that the memory
//    is allocated by new[] and hence an exception will be generated
//    if out of memory.
//
//    strndup_with_new() is the same as strdup_with_new() except that it will
//    copy up to the specified number of characters.  This function
//    is useful when we want to copy a substring out of a string
//    and didn't want to (or cannot) modify the string
// ----------------------------------------------------------------------
char* strndup_with_new(const char* the_string, int max_length) {
    if (the_string == nullptr) return nullptr;

    auto result = new char[max_length + 1];
    result[max_length] = '\0';  // terminate the string because strncpy might not
    return strncpy(result, the_string, max_length);
}

char* strdup_with_new(const char* the_string) {
    if (the_string == nullptr) {
        return nullptr;
    } else {
        return strndup_with_new(the_string, strlen(the_string));
    }
}

// ----------------------------------------------------------------------
// SplitCSVLineWithDelimiter()
//    CSV lines come in many guises.  There's the Comma Separated Values
//    variety, in which fields are separated by (surprise!) commas.  There's
//    also the tab-separated values variant, in which tabs separate the
//    fields.  This routine handles both, which makes it almost like
//    SplitUsing(line, delimiter), but for some special processing.  For both
//    delimiters, whitespace is trimmed from either side of the field value.
//    If the delimiter is ',', we play additional games with quotes.  A
//    field value surrounded by double quotes is allowed to contain commas,
//    which are not treated as field separators.  Within a double-quoted
//    string, a series of two double quotes signals an escaped single double
//    quote.  It'll be clearer in the examples.
//    Example:
//     Google , x , "Buchheit, Paul", "string with "" quote in it"
//     -->  [Google], [x], [Buchheit, Paul], [string with " quote in it]
//
// SplitCSVLine()
//    A convenience wrapper around SplitCSVLineWithDelimiter which uses
//    ',' as the delimiter.
//
// The following variants of SplitCSVLine() are not recommended for new code.
// Please consider the CSV parser in //util/csv as an alternative.  Examples:
// To parse a single line:
//     #include "util/csv/parser.h"
//     vector<string> fields = util::csv::ParseLine(line).fields();
//
// To parse an entire file:
//     #include "util/csv/parser.h"
//     for (Record rec : Parser(source)) {
//       vector<string> fields = rec.fields();
//     }
//
// See //util/csv/parser.h for more complete documentation.
//
// ----------------------------------------------------------------------
void SplitCSVLineWithDelimiter(char* line, char delimiter, std::vector<char*>* cols) {
    char* end_of_line = line + strlen(line);
    char* end;
    char* start;

    for (; line < end_of_line; line++) {
        // Skip leading whitespace, unless said whitespace is the delimiter.
        while (absl::ascii_isspace(*line) && *line != delimiter) ++line;

        if (*line == '"' && delimiter == ',') {  // Quoted value...
            start = ++line;
            end = start;
            for (; *line; line++) {
                if (*line == '"') {
                    line++;
                    if (*line != '"') {  // [""] is an escaped ["]
                        break;           // but just ["] is end of value
                    }
                }
                *end++ = *line;
            }
            // All characters after the closing quote and before the comma
            // are ignored.
            line = strchr(line, delimiter);
            if (!line) line = end_of_line;
        } else {
            start = line;
            line = strchr(line, delimiter);
            if (!line) line = end_of_line;
            // Skip all trailing whitespace, unless said whitespace is the delimiter.
            for (end = line; end > start; --end) {
                if (!absl::ascii_isspace(end[-1]) || end[-1] == delimiter) break;
            }
        }
        const bool need_another_column = (*line == delimiter) && (line == end_of_line - 1);
        *end = '\0';
        cols->push_back(start);
        // If line was something like [paul,] (comma is the last character
        // and is not proceeded by whitespace or quote) then we are about
        // to eliminate the last column (which is empty). This would be
        // incorrect.
        if (need_another_column) cols->push_back(end);

        assert(*line == '\0' || *line == delimiter);
    }
}

void SplitCSVLine(char* line, std::vector<char*>* cols) { SplitCSVLineWithDelimiter(line, ',', cols); }

void SplitCSVLineWithDelimiterForStrings(const std::string& line, char delimiter, std::vector<std::string>* cols) {
    // Unfortunately, the interface requires char* instead of const char*
    // which requires copying the string.
    char* cline = strndup_with_new(line.c_str(), line.size());
    std::vector<char*> v;
    SplitCSVLineWithDelimiter(cline, delimiter, &v);
    for (auto& ci : v) {
        cols->push_back(ci);
    }
    delete[] cline;
}

void SplitCSVLineForStrings(const std::string& line, std::vector<std::string>* cols) {
    SplitCSVLineWithDelimiterForStrings(line, ',', cols);
}

}  // namespace openmldb::cmd

#endif  // SRC_CMD_SPLIT_H_
