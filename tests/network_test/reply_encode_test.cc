#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "network/resp_parser.h"

namespace {

TEST(ReplyEncodeTest, EncodesNestedReplyTreeRecursively) {
  const minikv::ReplyNode reply = minikv::ReplyNode::Array(
      {minikv::ReplyNode::SimpleString("PONG"),
       minikv::ReplyNode::Array({minikv::ReplyNode::BulkString("field"),
                                 minikv::ReplyNode::BulkString("value")}),
       minikv::ReplyNode::Map(
           {{minikv::ReplyNode::BulkString("count"),
             minikv::ReplyNode::Integer(1)},
            {minikv::ReplyNode::BulkString("missing"),
             minikv::ReplyNode::Null()}})});

  EXPECT_EQ(minikv::EncodeReply(reply),
            "*3\r\n"
            "+PONG\r\n"
            "*2\r\n"
            "$5\r\nfield\r\n"
            "$5\r\nvalue\r\n"
            "%2\r\n"
            "$5\r\ncount\r\n"
            ":1\r\n"
            "$7\r\nmissing\r\n"
            "_\r\n");
}

TEST(ReplyEncodeTest, KeepsExistingSimpleIntegerAndArrayShapes) {
  minikv::CommandResponse ping;
  ping.status = rocksdb::Status::OK();
  ping.reply = minikv::ReplyNode::SimpleString("PONG");
  EXPECT_EQ(minikv::EncodeResponse(ping), "+PONG\r\n");

  minikv::CommandResponse hset;
  hset.status = rocksdb::Status::OK();
  hset.reply = minikv::ReplyNode::Integer(1);
  EXPECT_EQ(minikv::EncodeResponse(hset), ":1\r\n");

  minikv::CommandResponse hgetall;
  hgetall.status = rocksdb::Status::OK();
  hgetall.reply = minikv::ReplyNode::BulkStringArray({"name", "alice"});
  EXPECT_EQ(minikv::EncodeResponse(hgetall),
            "*2\r\n"
            "$4\r\nname\r\n"
            "$5\r\nalice\r\n");
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
