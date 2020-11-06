/*
 * ----------------------------------------------------------------------------
 * Copyright (c) 2013-2014, Leiwenbin
 * All rights reserved.
 * Distributed under GPL license.
 * ----------------------------------------------------------------------------
 */

#include <redis/xredis/xRedisClient.h>
#include <redis/xredis/xRedisPool.h>

using namespace xrcp;

bool xRedisClient::psubscribe(const SliceIndex& index, const KEYS& patterns, xRedisContext& ctx) {
    SETDEFAULTIOTYPE(MASTER)
    VDATA vCmdData;
    vCmdData.push_back("PSUBSCRIBE");
    addParam(vCmdData, patterns);
    return commandargv_array_ex(index, vCmdData, ctx);
}

bool xRedisClient::publish(const SliceIndex& index, const KEY& channel, const std::string& message, int64_t& count) {
    SETDEFAULTIOTYPE(MASTER)
    return command_integer(index, count, "PUBLISH %s %s", channel.c_str(), message.c_str(), count);
}

bool xRedisClient::pubsub_channels(const SliceIndex& index, const std::string& pattern, ArrayReply& reply) {
    SETDEFAULTIOTYPE(MASTER)
    return command_array(index, reply, "pubsub channels %s", pattern.c_str());
}

bool xRedisClient::pubsub_numsub(const SliceIndex& index, const KEYS& keys, ArrayReply& reply) {
    SETDEFAULTIOTYPE(MASTER)
    VDATA vCmdData;
    vCmdData.push_back("pubsub");
    vCmdData.push_back("numsub");
    addParam(vCmdData, keys);
    return commandargv_array(index, vCmdData, reply);
}

bool xRedisClient::pubsub_numpat(const SliceIndex& index, int64_t& count) {
    SETDEFAULTIOTYPE(MASTER)
    return command_integer(index, count, "pubsub numpat");
}

bool xRedisClient::punsubscribe(const SliceIndex& index, const KEYS& patterns, xRedisContext& ctx) {
    SETDEFAULTIOTYPE(MASTER)
    VDATA vCmdData;
    vCmdData.push_back("PUNSUBSCRIBE");
    addParam(vCmdData, patterns);

    bool bRet = false;
    RedisConn* pRedisConn = static_cast<RedisConn*>(ctx.conn);
    if (NULL == pRedisConn) {
        SetErrString(index, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    vector<const char*> argv(vCmdData.size());
    vector<size_t> argvlen(vCmdData.size());
    uint32_t j = 0;
    for (VDATA::const_iterator i = vCmdData.begin(); i != vCmdData.end(); ++i, ++j) {
        argv[j] = i->c_str(), argvlen[j] = i->size();
    }

    redisReply* reply = static_cast<redisReply*>(redisCommandArgv(pRedisConn->getCtx(), argv.size(), &(argv[0]), &(argvlen[0])));
    if (RedisPool::CheckReply(reply))
        bRet = true;
    else
        SetErrInfo(index, reply);
    RedisPool::FreeReply(reply);
    return bRet;
}

bool xRedisClient::subscribe(const SliceIndex& index, const KEYS& channels, xRedisContext& ctx) {
    SETDEFAULTIOTYPE(MASTER)
    VDATA vCmdData;
    vCmdData.push_back("SUBSCRIBE");
    addParam(vCmdData, channels);
    return commandargv_array_ex(index, vCmdData, ctx);
}

bool xRedisClient::unsubscribe(const SliceIndex& index, const KEYS& channels, xRedisContext& ctx) {
    SETDEFAULTIOTYPE(MASTER)
    VDATA vCmdData;
    vCmdData.push_back("UNSUBSCRIBE");
    addParam(vCmdData, channels);

    bool bRet = false;
    RedisConn* pRedisConn = static_cast<RedisConn*>(ctx.conn);
    if (NULL == pRedisConn) {
        SetErrString(index, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    vector<const char*> argv(vCmdData.size());
    vector<size_t> argvlen(vCmdData.size());
    uint32_t j = 0;
    for (VDATA::const_iterator i = vCmdData.begin(); i != vCmdData.end(); ++i, ++j) {
        argv[j] = i->c_str(), argvlen[j] = i->size();
    }

    redisReply* reply = static_cast<redisReply*>(redisCommandArgv(pRedisConn->getCtx(), argv.size(), &(argv[0]), &(argvlen[0])));
    if (RedisPool::CheckReply(reply))
        bRet = true;
    else
        SetErrInfo(index, reply);
    RedisPool::FreeReply(reply);
    return bRet;
}
