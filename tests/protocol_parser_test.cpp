#include "protocol_parser.h"

#include <gtest/gtest.h>

#include <variant>

using namespace cache_protocol;

TEST(ProtocolParserTest, CarriesPartialCommandIntoNextBuffer) {
    const ProtocolParser parser;
    const ParseResult first = parser.parse("SET key val");

    EXPECT_TRUE(first.items.empty());
    EXPECT_EQ(first.remainder, "SET key val");

    const ParseResult second = parser.parse(first.remainder + "ue 30\r\nGET key\r\n");
    ASSERT_EQ(second.items.size(), 2U);
    EXPECT_TRUE(second.remainder.empty());

    const auto* set = std::get_if<Command>(&second.items[0]);
    ASSERT_NE(set, nullptr);
    EXPECT_EQ(set->type, CommandType::Set);
    EXPECT_EQ(set->key, "key");
    EXPECT_EQ(set->value, "value");
    ASSERT_TRUE(set->ttl.has_value());
    EXPECT_EQ(set->ttl->count(), 30);

    const auto* get = std::get_if<Command>(&second.items[1]);
    ASSERT_NE(get, nullptr);
    EXPECT_EQ(get->type, CommandType::Get);
    EXPECT_EQ(get->key, "key");
}

TEST(ProtocolParserTest, ParsesPipelinedCommandsInOrder) {
    const ProtocolParser parser;
    const ParseResult result = parser.parse(
        "SET alpha one\nGET alpha\nDEL alpha\nGET alpha\n");

    ASSERT_EQ(result.items.size(), 4U);
    EXPECT_TRUE(result.remainder.empty());

    const CommandType expectedTypes[] = {
        CommandType::Set,
        CommandType::Get,
        CommandType::Del,
        CommandType::Get};
    for (std::size_t index = 0; index < result.items.size(); ++index) {
        const auto* command = std::get_if<Command>(&result.items[index]);
        ASSERT_NE(command, nullptr);
        EXPECT_EQ(command->type, expectedTypes[index]);
        EXPECT_EQ(command->key, "alpha");
    }
}

TEST(ProtocolParserTest, KeepsParseErrorsInCommandOrder) {
    const ProtocolParser parser;
    const ParseResult result = parser.parse("GET\nUNKNOWN key\nGET key\n");

    ASSERT_EQ(result.items.size(), 3U);
    const auto* firstError = std::get_if<ParseError>(&result.items[0]);
    ASSERT_NE(firstError, nullptr);
    EXPECT_EQ(firstError->message, "invalid arguments");
    const auto* secondError = std::get_if<ParseError>(&result.items[1]);
    ASSERT_NE(secondError, nullptr);
    EXPECT_EQ(secondError->message, "unknown command");
    EXPECT_TRUE(std::holds_alternative<Command>(result.items[2]));
}

TEST(ProtocolParserTest, FormatsProtocolResponses) {
    EXPECT_EQ(formatOkResponse(), "OK\n");
    EXPECT_EQ(formatValueResponse("value"), "value\n");
    EXPECT_EQ(formatNotFoundResponse(), "NOT_FOUND\n");
    EXPECT_EQ(formatErrorResponse("invalid arguments"), "ERR invalid arguments\n");
}

TEST(ProtocolParserTest, RejectsInvalidTtl) {
    const ProtocolParser parser;
    const ParseResult result = parser.parse("SET key value -1\n");

    ASSERT_EQ(result.items.size(), 1U);
    const auto* error = std::get_if<ParseError>(&result.items[0]);
    ASSERT_NE(error, nullptr);
    EXPECT_EQ(error->message, "invalid ttl");
}