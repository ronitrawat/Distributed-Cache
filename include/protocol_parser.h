#pragma once

#include <chrono>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace cache_protocol {

enum class CommandType {
    Set,
    Get,
    Del
};

struct Command {
    CommandType type;
    std::string key;
    std::string value;
    std::optional<std::chrono::seconds> ttl;
};

struct ParseError {
    std::string message;
};

using ParsedItem = std::variant<Command, ParseError>;

struct ParseResult {
    std::vector<ParsedItem> items;
    std::string remainder;
};

class ProtocolParser {
public:
    ParseResult parse(std::string_view bytes) const;
};

std::string formatOkResponse();
std::string formatValueResponse(std::string_view value);
std::string formatNotFoundResponse();
std::string formatErrorResponse(std::string_view message);

}  // namespace cache_protocol