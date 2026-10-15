#include "protocol_parser.h"

#include <charconv>
#include <cctype>
#include <system_error>

namespace cache_protocol {
namespace {

using TokenList = std::vector<std::string_view>;

TokenList splitTokens(std::string_view line) {
    TokenList tokens;
    std::size_t position = 0;

    while (position < line.size()) {
        while (position < line.size() &&
               std::isspace(static_cast<unsigned char>(line[position]))) {
            ++position;
        }

        const std::size_t tokenStart = position;
        while (position < line.size() &&
               !std::isspace(static_cast<unsigned char>(line[position]))) {
            ++position;
        }

        if (tokenStart != position) {
            tokens.push_back(line.substr(tokenStart, position - tokenStart));
        }
    }

    return tokens;
}

ParsedItem parseLine(std::string_view line) {
    const TokenList tokens = splitTokens(line);
    if (tokens.empty()) {
        return ParseError{"empty command"};
    }

    if (tokens[0] == "SET") {
        if (tokens.size() != 3 && tokens.size() != 4) {
            return ParseError{"invalid arguments"};
        }

        std::optional<std::chrono::seconds> ttl;
        if (tokens.size() == 4) {
            std::chrono::seconds::rep ttlSeconds = 0;
            const auto* ttlStart = tokens[3].data();
            const auto* ttlEnd = ttlStart + tokens[3].size();
            const auto [parsedEnd, error] =
                std::from_chars(ttlStart, ttlEnd, ttlSeconds);
            if (error != std::errc{} || parsedEnd != ttlEnd || ttlSeconds < 0) {
                return ParseError{"invalid ttl"};
            }
            ttl = std::chrono::seconds(ttlSeconds);
        }

        return Command{
            CommandType::Set,
            std::string(tokens[1]),
            std::string(tokens[2]),
            ttl};
    }

    if (tokens[0] == "GET" || tokens[0] == "DEL") {
        if (tokens.size() != 2) {
            return ParseError{"invalid arguments"};
        }

        const CommandType type =
            tokens[0] == "GET" ? CommandType::Get : CommandType::Del;
        return Command{type, std::string(tokens[1]), {}, std::nullopt};
    }

    return ParseError{"unknown command"};
}

}  // namespace

ParseResult ProtocolParser::parse(std::string_view bytes) const {
    ParseResult result;
    std::size_t lineStart = 0;

    while (true) {
        const std::size_t newline = bytes.find('\n', lineStart);
        if (newline == std::string_view::npos) {
            result.remainder = std::string(bytes.substr(lineStart));
            break;
        }

        std::string_view line = bytes.substr(lineStart, newline - lineStart);
        if (!line.empty() && line.back() == '\r') {
            line.remove_suffix(1);
        }
        result.items.push_back(parseLine(line));
        lineStart = newline + 1;
    }

    return result;
}

std::string formatOkResponse() {
    return "OK\n";
}

std::string formatValueResponse(std::string_view value) {
    std::string response(value);
    response.push_back('\n');
    return response;
}

std::string formatNotFoundResponse() {
    return "NOT_FOUND\n";
}

std::string formatErrorResponse(std::string_view message) {
    std::string response = "ERR ";
    response.append(message);
    response.push_back('\n');
    return response;
}

}  // namespace cache_protocol