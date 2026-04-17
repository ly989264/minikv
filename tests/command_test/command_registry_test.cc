#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "kernel/command_registry.h"

namespace {

class DummyCmd : public minikv::Cmd {
 public:
  explicit DummyCmd(const minikv::CmdRegistration& registration)
      : minikv::Cmd(registration.name, registration.flags) {}

 private:
  rocksdb::Status DoInitial(const minikv::CmdInput& /*input*/) override {
    return rocksdb::Status::OK();
  }

  minikv::CommandResponse Do(minikv::CommandServices* /*context*/) override {
    return MakeSimpleString("OK");
  }
};

std::unique_ptr<minikv::Cmd> CreateDummyCmd(
    const minikv::CmdRegistration& registration) {
  return std::make_unique<DummyCmd>(registration);
}

TEST(CommandRegistryTest, RejectsCaseInsensitiveNameCollisions) {
  minikv::CommandRegistry registry;
  ASSERT_TRUE(registry
                  .Register({"ping", minikv::CmdFlags::kRead,
                             minikv::CommandSource::kBuiltin, &CreateDummyCmd})
                  .ok());

  rocksdb::Status status =
      registry.Register({"PING", minikv::CmdFlags::kFast,
                         minikv::CommandSource::kModule, &CreateDummyCmd});
  ASSERT_TRUE(status.IsInvalidArgument());
  EXPECT_NE(status.ToString().find("PING"), std::string::npos);
}

TEST(CommandRegistryTest, StoresNormalizedCommandMetadata) {
  minikv::CommandRegistry registry;
  ASSERT_TRUE(registry
                  .Register({"hscan", minikv::CmdFlags::kRead | minikv::CmdFlags::kSlow,
                             minikv::CommandSource::kModule, &CreateDummyCmd})
                  .ok());

  const minikv::CmdRegistration* registration = registry.Find("HSCAN");
  ASSERT_NE(registration, nullptr);
  EXPECT_EQ(registration->name, "HSCAN");
  EXPECT_EQ(registration->source, minikv::CommandSource::kModule);
  EXPECT_TRUE(minikv::HasFlag(registration->flags, minikv::CmdFlags::kRead));
  EXPECT_TRUE(minikv::HasFlag(registration->flags, minikv::CmdFlags::kSlow));
}

}  // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
