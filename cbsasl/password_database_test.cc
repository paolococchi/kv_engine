/*
 *     Copyright 2016 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "password_database.h"
#include "user.h"

#include <cbcrypto/cbcrypto.h>
#include <cbsasl/pwdb.h>
#include <cbsasl/server.h>
#include <folly/portability/GTest.h>
#include <nlohmann/json.hpp>
#include <openssl/evp.h>
#include <platform/base64.h>
#include <platform/checked_snprintf.h>
#include <platform/dirutils.h>
#include <platform/random.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>

class PasswordMetaTest : public ::testing::Test {
public:
    void SetUp() {
        root["h"] = "NP0b1Ji5jWG/ZV6hPzOIk3lmTmw=";
        root["s"] = "iiU7hLv7l3yOoEgXusJvT2i1J2A=";
        root["i"] = 10;
    }

    nlohmann::json root;
};

TEST_F(PasswordMetaTest, TestNormalInit) {
    cb::sasl::pwdb::User::PasswordMetaData md;
    EXPECT_NO_THROW(md = cb::sasl::pwdb::User::PasswordMetaData(root));
    EXPECT_EQ("iiU7hLv7l3yOoEgXusJvT2i1J2A=", md.getSalt());
    EXPECT_EQ("NP0b1Ji5jWG/ZV6hPzOIk3lmTmw=",
              Couchbase::Base64::encode(md.getPassword()));
    EXPECT_EQ(10, md.getIterationCount());
}

TEST_F(PasswordMetaTest, UnknownLabel) {
    root["extra"] = "foo";
    EXPECT_THROW(cb::sasl::pwdb::User::PasswordMetaData md(root),
                 std::runtime_error);
}

TEST_F(PasswordMetaTest, TestMissingHash) {
    root.erase("h");
    EXPECT_THROW(cb::sasl::pwdb::User::PasswordMetaData md(root),
                 std::runtime_error);
}

TEST_F(PasswordMetaTest, TestInvalidDatatypeForHash) {
    root["h"] = 5;
    EXPECT_THROW(cb::sasl::pwdb::User::PasswordMetaData md(root),
                 std::runtime_error);
}

TEST_F(PasswordMetaTest, TestMissingSalt) {
    root.erase("s");
    EXPECT_THROW(cb::sasl::pwdb::User::PasswordMetaData md(root),
                 std::runtime_error);
}

TEST_F(PasswordMetaTest, TestInvalidDatatypeForSalt) {
    root["s"] = 5;
    EXPECT_THROW(cb::sasl::pwdb::User::PasswordMetaData md(root),
                 std::runtime_error);
}

TEST_F(PasswordMetaTest, TestMissingIterationCount) {
    root.erase("i");
    EXPECT_THROW(cb::sasl::pwdb::User::PasswordMetaData md(root),
                 std::runtime_error);
}

TEST_F(PasswordMetaTest, TestInvalidDatatypeForIterationCount) {
    root["i"] = "foo";
    EXPECT_THROW(cb::sasl::pwdb::User::PasswordMetaData md(root),
                 std::runtime_error);
}

TEST_F(PasswordMetaTest, TestInvalidBase64EncodingForHash) {
    root["h"] = "!@#$%^&*";
    EXPECT_THROW(cb::sasl::pwdb::User::PasswordMetaData md(root),
                 std::invalid_argument);
}

TEST_F(PasswordMetaTest, TestInvalidBase64EncodingForSalt) {
    root["s"] = "!@#$%^&*";
    EXPECT_THROW(cb::sasl::pwdb::User::PasswordMetaData md(root),
                 std::invalid_argument);
}

class UserTest : public ::testing::Test {
public:
    void SetUp() {
        root["n"] = "username";
        root["plain"] = Couchbase::Base64::encode("secret");

        nlohmann::json sha1;
        sha1["h"] = "NP0b1Ji5jWG/ZV6hPzOIk3lmTmw=";
        sha1["s"] = "iiU7hLv7l3yOoEgXusJvT2i1J2A=";
        sha1["i"] = 10;
        root["sha1"] = sha1;

        nlohmann::json sha256;
        sha256["h"] = "BGq4Rd/YH5nfqeV2CtL0lTBLZezuBQVpdTHDGFAwW8w=";
        sha256["s"] = "i5Jn//LLM0245cscYnldCjM/HMC7Hj2U1HT6iXqCC0E=";
        sha256["i"] = 10;
        root["sha256"] = sha256;

        nlohmann::json sha512;
        sha512["h"] =
                "KZuRjeXbF6NR5rrrQMyHAOvkFq7dUSQ6H08uV"
                "ae6TPUTKs4DZNSCenq+puXq5t9zrW9oZb"
                "Ic/6wUODFh3ZKAOQ==";
        sha512["s"] =
                "nUNk2ZbAZTabxboF+OBQws3zNJpxePtnuF8Kw"
                "cylC3h/NnQQ9FqU0YYohjJhvGRNbxjPTT"
                "SuYOgxBG4FMV1W3A==";
        sha512["i"] = 10;
        root["sha512"] = sha512;
    }

    nlohmann::json root;
};

TEST_F(UserTest, TestNormalInit) {
    using namespace cb::sasl;
    pwdb::User u;
    EXPECT_NO_THROW(u = pwdb::UserFactory::create(root));
    EXPECT_EQ("username", u.getUsername().getRawValue());
    EXPECT_NO_THROW(u.getPassword(Mechanism::SCRAM_SHA512));
    EXPECT_NO_THROW(u.getPassword(Mechanism::SCRAM_SHA256));
    EXPECT_NO_THROW(u.getPassword(Mechanism::SCRAM_SHA1));
    EXPECT_NO_THROW(u.getPassword(Mechanism::PLAIN));

    {
        auto& md = u.getPassword(Mechanism::SCRAM_SHA512);
        EXPECT_EQ(10, md.getIterationCount());
        EXPECT_EQ(
                "nUNk2ZbAZTabxboF+OBQws3zNJpxePtnuF8Kw"
                "cylC3h/NnQQ9FqU0YYohjJhvGRNbxjPTT"
                "SuYOgxBG4FMV1W3A==",
                md.getSalt());
        EXPECT_EQ(
                "KZuRjeXbF6NR5rrrQMyHAOvkFq7dUSQ6H08uV"
                "ae6TPUTKs4DZNSCenq+puXq5t9zrW9oZb"
                "Ic/6wUODFh3ZKAOQ==",
                Couchbase::Base64::encode(md.getPassword()));
    }

    {
        auto& md = u.getPassword(Mechanism::SCRAM_SHA256);
        EXPECT_EQ(10, md.getIterationCount());
        EXPECT_EQ("i5Jn//LLM0245cscYnldCjM/HMC7Hj2U1HT6iXqCC0E=", md.getSalt());
        EXPECT_EQ("BGq4Rd/YH5nfqeV2CtL0lTBLZezuBQVpdTHDGFAwW8w=",
                  Couchbase::Base64::encode(md.getPassword()));
    }

    {
        auto& md = u.getPassword(Mechanism::SCRAM_SHA1);
        EXPECT_EQ(10, md.getIterationCount());
        EXPECT_EQ("iiU7hLv7l3yOoEgXusJvT2i1J2A=", md.getSalt());
        EXPECT_EQ("NP0b1Ji5jWG/ZV6hPzOIk3lmTmw=",
                  Couchbase::Base64::encode(md.getPassword()));
    }

    {
        auto& md = u.getPassword(Mechanism::PLAIN);
        EXPECT_EQ(0, md.getIterationCount());
        EXPECT_EQ("", md.getSalt());
        EXPECT_EQ("secret", md.getPassword());
    }
}

TEST_F(UserTest, TestNoPlaintext) {
    using namespace cb::sasl;

    root.erase("plain");
    pwdb::User u;
    EXPECT_NO_THROW(u = pwdb::UserFactory::create(root));
    EXPECT_NO_THROW(u.getPassword(Mechanism::SCRAM_SHA512));
    EXPECT_NO_THROW(u.getPassword(Mechanism::SCRAM_SHA256));
    EXPECT_NO_THROW(u.getPassword(Mechanism::SCRAM_SHA1));
    EXPECT_THROW(u.getPassword(Mechanism::PLAIN), std::invalid_argument);
}

TEST_F(UserTest, TestNoSha512) {
    using namespace cb::sasl;

    root.erase("sha512");
    pwdb::User u;
    EXPECT_NO_THROW(u = pwdb::UserFactory::create(root));
    EXPECT_THROW(u.getPassword(Mechanism::SCRAM_SHA512), std::invalid_argument);
    EXPECT_NO_THROW(u.getPassword(Mechanism::SCRAM_SHA256));
    EXPECT_NO_THROW(u.getPassword(Mechanism::SCRAM_SHA1));
    EXPECT_NO_THROW(u.getPassword(Mechanism::PLAIN));
}

TEST_F(UserTest, TestNoSha256) {
    using namespace cb::sasl;

    root.erase("sha256");
    pwdb::User u;
    EXPECT_NO_THROW(u = pwdb::UserFactory::create(root));
    EXPECT_THROW(u.getPassword(Mechanism::SCRAM_SHA256), std::invalid_argument);
    EXPECT_NO_THROW(u.getPassword(Mechanism::SCRAM_SHA512));
    EXPECT_NO_THROW(u.getPassword(Mechanism::SCRAM_SHA1));
    EXPECT_NO_THROW(u.getPassword(Mechanism::PLAIN));
}

TEST_F(UserTest, TestNoSha1) {
    using namespace cb::sasl;

    root.erase("sha1");
    pwdb::User u;
    EXPECT_NO_THROW(u = pwdb::UserFactory::create(root));
    EXPECT_THROW(u.getPassword(Mechanism::SCRAM_SHA1), std::invalid_argument);
    EXPECT_NO_THROW(u.getPassword(Mechanism::SCRAM_SHA512));
    EXPECT_NO_THROW(u.getPassword(Mechanism::SCRAM_SHA256));
    EXPECT_NO_THROW(u.getPassword(Mechanism::PLAIN));
}

TEST_F(UserTest, InvalidLabel) {
    root["gssapi"] = "foo";
    EXPECT_THROW(auto u = cb::sasl::pwdb::UserFactory::create(root),
                 std::runtime_error);
}

/**
 * Make sure that we generate the dummy salts the same way as ns_server does.
 *
 * The fallback salt and the resulting salt were reported back from the
 * ns_server team so we can verify that we generate the same salt by using
 * the same input data
 */
TEST_F(UserTest, CreateDummy) {
    using namespace cb::sasl;
    // set the fallback salt to something we know about ;)
    cb::sasl::server::set_scramsha_fallback_salt("WyulJ+YpKKZn+y9f");
    auto u = pwdb::UserFactory::createDummy("foobar", Mechanism::SCRAM_SHA512);
    EXPECT_TRUE(u.isDummy());
    auto meta = u.getPassword(Mechanism::SCRAM_SHA512);
    EXPECT_EQ(
            "ZLBvongMC+gVSc8JsnCmK8CE+KJrCdS/8fT4cvb3IkJJGTgaGQ+HGuQaXKTN9829l/"
            "8eoUUpiI2Cyk/CRnULtw==",
            meta.getSalt());
}

class PasswordDatabaseTest : public ::testing::Test {
public:
    void SetUp() {
        nlohmann::json root;
        nlohmann::json array = nlohmann::json::array();

        array.push_back(cb::sasl::pwdb::UserFactory::create("trond", "secret1")
                                .to_json());
        array.push_back(cb::sasl::pwdb::UserFactory::create("mike", "secret2")
                                .to_json());
        array.push_back(cb::sasl::pwdb::UserFactory::create("anne", "secret3")
                                .to_json());
        array.push_back(cb::sasl::pwdb::UserFactory::create("will", "secret4")
                                .to_json());
        array.push_back(cb::sasl::pwdb::UserFactory::create("dave", "secret5")
                                .to_json());

        root["users"] = array;

        json = root.dump();
    }

    std::string json;
};

TEST_F(PasswordDatabaseTest, TestNormalInit) {
    cb::sasl::pwdb::PasswordDatabase db;
    EXPECT_NO_THROW(db = cb::sasl::pwdb::PasswordDatabase(json, false));

    EXPECT_FALSE(db.find("trond").isDummy());
    EXPECT_FALSE(db.find("mike").isDummy());
    EXPECT_FALSE(db.find("anne").isDummy());
    EXPECT_FALSE(db.find("will").isDummy());
    EXPECT_FALSE(db.find("dave").isDummy());
    EXPECT_TRUE(db.find("unknown").isDummy());
}

TEST_F(PasswordDatabaseTest, EmptyConstructor) {
    EXPECT_NO_THROW(cb::sasl::pwdb::PasswordDatabase db);
}

TEST_F(PasswordDatabaseTest, DetectIllegalLabel) {
    EXPECT_THROW(cb::sasl::pwdb::PasswordDatabase db("{ \"foo\": [] }", false),
                 std::runtime_error);
}

TEST_F(PasswordDatabaseTest, DetectIllegalUsersType) {
    EXPECT_THROW(
            cb::sasl::pwdb::PasswordDatabase db("{ \"users\": 24 }", false),
            std::runtime_error);
}

TEST_F(PasswordDatabaseTest, CreateFromJsonDatabaseNoUsers) {
    cb::sasl::pwdb::PasswordDatabase db;
    EXPECT_NO_THROW(
            db = cb::sasl::pwdb::PasswordDatabase("{ \"users\": [] }", false));

    EXPECT_TRUE(db.find("trond").isDummy());
    EXPECT_TRUE(db.find("unknown").isDummy());
}

TEST_F(PasswordDatabaseTest, CreateFromJsonDatabaseExtraLabel) {
    EXPECT_THROW(cb::sasl::pwdb::PasswordDatabase db(
                         "{ \"users\": [], \"foo\", 2 }", false),
                 std::runtime_error);
}

static char environment[1024];

class EncryptedDatabaseTest : public ::testing::Test {
public:
    void SetUp() override {
        nlohmann::json meta;
        meta["cipher"] = "AES_256_cbc";
        std::string blob;
        blob.resize(EVP_CIPHER_key_length(EVP_aes_256_cbc()));

        cb::RandomGenerator randomGenerator;
        ASSERT_TRUE(randomGenerator.getBytes(const_cast<char*>(blob.data()),
                                             blob.size()));
        meta["key"] = Couchbase::Base64::encode(blob);

        blob.resize(EVP_CIPHER_iv_length(EVP_aes_256_cbc()));
        ASSERT_TRUE(randomGenerator.getBytes(const_cast<char*>(blob.data()),
                                             blob.size()));

        meta["iv"] = Couchbase::Base64::encode(blob);

        std::string envstr = meta.dump();

        // Add the file to the exec environment
        checked_snprintf(environment,
                         sizeof(environment),
                         "COUCHBASE_CBSASL_SECRETS=%s",
                         envstr.c_str());

        filename = cb::io::mktemp("./cryptfile.");
    }

    void TearDown() override {
#ifdef _MSC_VER
        checked_snprintf(
                environment, sizeof(environment), "COUCHBASE_CBSASL_SECRETS=");
        putenv(environment);
#else
        unsetenv("COUCHBASE_CBSASL_SECRETS");
#endif
        EXPECT_NO_THROW(cb::io::rmrf(filename));
    }

protected:
    std::string filename;
};

TEST_F(EncryptedDatabaseTest, WriteReadFilePlain) {
    EXPECT_EQ(nullptr, getenv("COUCHBASE_CBSASL_SECRETS"));
    const std::string input{"All work and no play makes Jack a dull boy"};
    cb::sasl::pwdb::write_password_file(filename, input);
    auto content = cb::sasl::pwdb::read_password_file(filename);
    EXPECT_EQ(input, content);
}

TEST_F(EncryptedDatabaseTest, WriteReadFileEncrypted) {
    putenv(environment);
    const std::string input{"All work and no play makes Jack a dull boy"};
    cb::sasl::pwdb::write_password_file(filename, input);
    auto content = cb::sasl::pwdb::read_password_file(filename);
    EXPECT_EQ(input, content);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);

    // Reduce the iteration cycles in HMAC to be nicer to valgrind ;-)
    cb::sasl::pwdb::UserFactory::setDefaultHmacIterationCount(10);

    return RUN_ALL_TESTS();
}
