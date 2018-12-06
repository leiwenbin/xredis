/*
 * ----------------------------------------------------------------------------
 * Copyright (c) 2013-2014, Leiwenbin
 * All rights reserved.
 * Distributed under GPL license.
 * ----------------------------------------------------------------------------
 */

#include <sstream>
#include <redis/xredis/xRedisClient.h>

bool xRedisClient::lindex(const RedisDBIdx& dbi, const string& key, int64_t index, VALUE& value) {
    if (0 == key.length()) return false;
    SETDEFAULTIOTYPE(SLAVE);
    return command_string(dbi, value, "LINDEX %s %lld", key.c_str(), index);
}

bool xRedisClient::linsert(const RedisDBIdx& dbi, const string& key, const LMODEL mod, const string& pivot, const string& value, int64_t& retval) {
    static const char* lmodel[2] = {"BEFORE", "AFTER"};
    if (0 == key.length()) return false;
    SETDEFAULTIOTYPE(MASTER);
    return command_integer(dbi, retval, "LINSERT %s %s %s %s", key.c_str(), lmodel[mod], pivot.c_str(), value.c_str());
}

bool xRedisClient::llen(const RedisDBIdx& dbi, const string& key, int64_t& retval) {
    if (0 == key.length()) return false;
    SETDEFAULTIOTYPE(SLAVE);
    return command_integer(dbi, retval, "LLEN %s", key.c_str());
}

bool xRedisClient::lpop(const RedisDBIdx& dbi, const string& key, string& value) {
    if (0 == key.length()) return false;
    SETDEFAULTIOTYPE(MASTER);
    return command_string(dbi, value, "LPOP %s", key.c_str());
}

bool xRedisClient::lpush(const RedisDBIdx& dbi, const string& key, const VALUES& vValue, int64_t& length) {
    if (0 == key.length()) return false;
    VDATA vCmdData;
    vCmdData.push_back("LPUSH");
    vCmdData.push_back(key);
    addparam(vCmdData, vValue);
    SETDEFAULTIOTYPE(MASTER);
    return commandargv_integer(dbi, vCmdData, length);
}

bool xRedisClient::lpushx(const RedisDBIdx& dbi, const string& key, const string& value, int64_t& length) {
    if (0 == key.length()) return false;
    SETDEFAULTIOTYPE(MASTER);
    return command_integer(dbi, length, "LPUSHX %s %s", key.c_str(), value.c_str());
}

bool xRedisClient::blpop(const RedisDBIdx& dbi, const std::string& key, VALUES& values, int64_t& timeout) {
    if (0 == key.length()) return false;
    SETDEFAULTIOTYPE(SLAVE);
    return command_list(dbi, values, "BLPOP %s %ld", key.c_str(), timeout);
}

bool xRedisClient::lrange(const RedisDBIdx& dbi, const string& key, int64_t start, int64_t end, ArrayReply& array) {
    if (0 == key.length()) return false;
    SETDEFAULTIOTYPE(SLAVE);
    return command_array(dbi, array, "LRANGE %s %lld %lld", key.c_str(), start, end);
}

bool xRedisClient::lrem(const RedisDBIdx& dbi, const string& key, int32_t count, const string& value, int64_t num) {
    if (0 == key.length()) return false;
    SETDEFAULTIOTYPE(MASTER);
    return command_integer(dbi, num, "LREM %s %d %s", key.c_str(), count, value.c_str());
}

bool xRedisClient::lset(const RedisDBIdx& dbi, const string& key, int32_t index, const string& value) {
    if (0 == key.length()) return false;
    SETDEFAULTIOTYPE(MASTER);
    return command_status(dbi, "LSET %s %d %s", key.c_str(), index, value.c_str());
}

bool xRedisClient::ltrim(const RedisDBIdx& dbi, const string& key, int32_t start, int32_t end) {
    if (0 == key.length()) return false;
    SETDEFAULTIOTYPE(MASTER);
    return command_status(dbi, "LTRIM %s %d %d", key.c_str(), start, end);
}

bool xRedisClient::rpop(const RedisDBIdx& dbi, const string& key, string& value) {
    if (0 == key.length()) return false;
    SETDEFAULTIOTYPE(MASTER);
    return command_string(dbi, value, "RPOP %s", key.c_str());
}

bool xRedisClient::rpoplpush(const RedisDBIdx& dbi, const string& key_src, const string& key_dest, string& value) {
    if ((0 == key_src.length()) || (0 == key_dest.length())) return false;
    SETDEFAULTIOTYPE(MASTER);
    return command_string(dbi, value, "RPOPLPUSH %s %s", key_src.c_str(), key_dest.c_str());
}

bool xRedisClient::rpush(const RedisDBIdx& dbi, const string& key, const VALUES& vValue, int64_t& length) {
    if (0 == key.length()) return false;
    VDATA vCmdData;
    vCmdData.push_back("RPUSH");
    vCmdData.push_back(key);
    addparam(vCmdData, vValue);
    SETDEFAULTIOTYPE(MASTER);
    return commandargv_integer(dbi, vCmdData, length);
}

bool xRedisClient::rpushx(const RedisDBIdx& dbi, const string& key, const string& value, int64_t& length) {
    if (0 == key.length()) return false;
    SETDEFAULTIOTYPE(MASTER);
    return command_integer(dbi, length, "RPUSHX %s %s", key.c_str(), value.c_str());
}

bool xRedisClient::brpop(const RedisDBIdx& dbi, const std::string& key, VALUES& values, int64_t& timeout) {
    if (0 == key.length()) return false;
    SETDEFAULTIOTYPE(SLAVE);
    return command_list(dbi, values, "BRPOP %s %ld", key.c_str(), timeout);
}

