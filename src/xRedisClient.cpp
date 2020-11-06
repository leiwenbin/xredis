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

SliceIndex::SliceIndex(xRedisClient* xRedisClient, uint32_t nodeIndex) {
    mNodeIndex = nodeIndex;
    mSliceIndex = 0;
    mClient = xRedisClient;
    mIOtype = MASTER;
    mIOFlag = false;
}

SliceIndex::~SliceIndex() {
}

bool SliceIndex::Create(const char* key, xrcp::HASHFUN fun) {
    uint32_t sliceCount = mClient->GetRedisPool()->GetSliceCount(mNodeIndex);
    if ((NULL != fun) && (sliceCount > 0)) {
        mSliceIndex = fun(key) % sliceCount;
        return true;
    }
    return false;
}

bool SliceIndex::CreateByID(int64_t id) {
    uint32_t sliceCount = mClient->GetRedisPool()->GetSliceCount(mNodeIndex);
    if (sliceCount > 0) {
        mSliceIndex = (uint32_t) (id % sliceCount);
        return true;
    }
    return false;
}

uint32_t SliceIndex::GetSliceIndex() {
    return mSliceIndex;
}

void SliceIndex::IOtype(uint32_t ioType) {
    mIOtype = ioType;
}

void SliceIndex::SetIOMaster() {
    mIOtype = MASTER;
    mIOFlag = true;
}

bool SliceIndex::SetErrInfo(const char* info, size_t len) {
    mStrerr.clear();
    if (NULL == info) return false;
    mStrerr.assign(info, len);
    return true;
}

xRedisClient::xRedisClient() {
    mRedisPool = NULL;
}

xRedisClient::~xRedisClient() {
    Release();
}

bool xRedisClient::Init(uint32_t nodeCount) {
    if (NULL == mRedisPool) {
        mRedisPool = new RedisPool;
        if (NULL == mRedisPool) return false;

        return mRedisPool->Init(nodeCount);
    }
    return false;
}

void xRedisClient::Release() {
    if (NULL != mRedisPool) {
        mRedisPool->Release();
        delete mRedisPool;
        mRedisPool = NULL;
    }
}

void xRedisClient::Keepalive() {
    if (NULL != mRedisPool)
        mRedisPool->Keepalive();
}

inline RedisPool* xRedisClient::GetRedisPool() {
    return mRedisPool;
}

bool xRedisClient::ConnectRedisCache(RedisNode* redisNodeList, uint32_t redisNodeCount, uint32_t nodeIndex) {
    if (NULL == mRedisPool) return false;

    if (!mRedisPool->SetSliceCount(nodeIndex, redisNodeCount)) return false;

    for (uint32_t sliceIndex = 0; sliceIndex < redisNodeCount; sliceIndex++) {
        RedisNode* pNode = &redisNodeList[sliceIndex];
        if (NULL == pNode) return false;
        pNode->sliceIndex = sliceIndex;

        if (!mRedisPool->ConnectRedisDB(nodeIndex, pNode->sliceIndex, pNode->host, pNode->port, pNode->passwd, pNode->poolSize, pNode->timeout, pNode->role)) {
            return false;
        }
    }

    return true;
}

void xRedisClient::addParam(VDATA& vDes, const VDATA& vSrc) {
    for (VDATA::const_iterator iter = vSrc.begin(); iter != vSrc.end(); ++iter) {
        vDes.push_back(*iter);
    }
}

void xRedisClient::SetErrInfo(const SliceIndex& index, void* p) {
    if (NULL == p) {
        SetErrString(index, CONNECT_CLOSED_ERROR, ::strlen(CONNECT_CLOSED_ERROR));
    } else {
        redisReply* reply = (redisReply*) p;
        SetErrString(index, reply->str, static_cast<int32_t>(reply->len));
    }
}

void xRedisClient::SetErrString(const SliceIndex& index, const char* str, size_t len) {
    SliceIndex& sliceIndex = const_cast<SliceIndex&>(index);
    sliceIndex.SetErrInfo(str, len);
}

void xRedisClient::SetIOtype(const SliceIndex& index, uint32_t ioType, bool ioFlag) {
    SliceIndex& sliceIndex = const_cast<SliceIndex&>(index);
    sliceIndex.IOtype(ioType);
    sliceIndex.mIOFlag = ioFlag;
}

void xRedisClient::SetErrMessage(const SliceIndex& index, const char* fmt, ...) {
    char szBuf[128] = {0};
    va_list va;
    va_start(va, fmt);
    vsnprintf(szBuf, sizeof(szBuf), fmt, va);
    va_end(va);
    SetErrString(index, szBuf, ::strlen(szBuf));
}

rReply* xRedisClient::command(const SliceIndex& index, const char* cmd) {
    RedisConn* pRedisConn = mRedisPool->GetConnection(index.mNodeIndex, index.mSliceIndex, index.mIOtype);
    if (NULL == pRedisConn) {
        SetErrString(index, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return NULL;
    }
    rReply* reply = static_cast<rReply*>(redisCommand(pRedisConn->getCtx(), cmd));

    mRedisPool->FreeConnection(pRedisConn);
    return reply;
}

bool xRedisClient::command_bool(const SliceIndex& index, const char* cmd, ...) {
    bool bRet = false;
    RedisConn* pRedisConn = mRedisPool->GetConnection(index.mNodeIndex, index.mSliceIndex, index.mIOtype);
    if (NULL == pRedisConn) {
        SetErrString(index, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    va_list args;
    va_start(args, cmd);
    redisReply* reply = static_cast<redisReply*>(redisvCommand(pRedisConn->getCtx(), cmd, args));
    va_end(args);

    if (RedisPool::CheckReply(reply)) {
        if (REDIS_REPLY_STATUS == reply->type)
            bRet = true;
        else if (REDIS_REPLY_INTEGER == reply->type)
            bRet = reply->integer == 1;
    } else {
        SetErrInfo(index, reply);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);

    return bRet;
}

bool xRedisClient::command_status(const SliceIndex& index, const char* cmd, ...) {
    bool bRet = false;
    RedisConn* pRedisConn = mRedisPool->GetConnection(index.mNodeIndex, index.mSliceIndex, index.mIOtype);
    if (NULL == pRedisConn) {
        SetErrString(index, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    va_list args;
    va_start(args, cmd);
    redisReply* reply = static_cast<redisReply*>(redisvCommand(pRedisConn->getCtx(), cmd, args));
    va_end(args);

    if (RedisPool::CheckReply(reply)) {
        // Assume good reply until further inspection
        bRet = true;

        if (REDIS_REPLY_STRING == reply->type) {
            if (!reply->len || !reply->str || strcasecmp(reply->str, "OK") != 0)
                bRet = false;
        }
    } else {
        SetErrInfo(index, reply);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);

    return bRet;
}

bool xRedisClient::command_integer(const SliceIndex& index, int64_t& retval, const char* cmd, ...) {
    bool bRet = false;
    RedisConn* pRedisConn = mRedisPool->GetConnection(index.mNodeIndex, index.mSliceIndex, index.mIOtype);
    if (NULL == pRedisConn) {
        SetErrString(index, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    va_list args;
    va_start(args, cmd);
    redisReply* reply = static_cast<redisReply*>(redisvCommand(pRedisConn->getCtx(), cmd, args));
    va_end(args);
    if (RedisPool::CheckReply(reply)) {
        retval = reply->integer;
        bRet = true;
    } else {
        SetErrInfo(index, reply);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);

    return bRet;
}

bool xRedisClient::command_string(const SliceIndex& index, string& data, const char* cmd, ...) {
    bool bRet = false;
    RedisConn* pRedisConn = mRedisPool->GetConnection(index.mNodeIndex, index.mSliceIndex, index.mIOtype);
    if (NULL == pRedisConn) {
        SetErrString(index, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    va_list args;
    va_start(args, cmd);
    redisReply* reply = static_cast<redisReply*>(redisvCommand(pRedisConn->getCtx(), cmd, args));
    va_end(args);
    if (RedisPool::CheckReply(reply)) {
        data.assign(reply->str, reply->len);
        bRet = true;
    } else {
        SetErrInfo(index, reply);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);

    return bRet;
}

bool xRedisClient::command_list(const SliceIndex& index, VALUES& vValue, const char* cmd, ...) {
    bool bRet = false;
    RedisConn* pRedisConn = mRedisPool->GetConnection(index.mNodeIndex, index.mSliceIndex, index.mIOtype);
    if (NULL == pRedisConn) {
        SetErrString(index, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    va_list args;
    va_start(args, cmd);
    redisReply* reply = static_cast<redisReply*>(redisvCommand(pRedisConn->getCtx(), cmd, args));
    va_end(args);
    if (RedisPool::CheckReply(reply)) {
        for (size_t i = 0; i < reply->elements; i++) {
            vValue.push_back(string(reply->element[i]->str, reply->element[i]->len));
        }
        bRet = true;
    } else {
        SetErrInfo(index, reply);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);

    return bRet;
}

bool xRedisClient::command_array(const SliceIndex& index, ArrayReply& array, const char* cmd, ...) {
    bool bRet = false;
    RedisConn* pRedisConn = mRedisPool->GetConnection(index.mNodeIndex, index.mSliceIndex, index.mIOtype);
    if (NULL == pRedisConn) {
        SetErrString(index, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    va_list args;
    va_start(args, cmd);
    redisReply* reply = static_cast<redisReply*>(redisvCommand(pRedisConn->getCtx(), cmd, args));
    va_end(args);
    if (RedisPool::CheckReply(reply)) {
        for (size_t i = 0; i < reply->elements; i++) {
            DataItem item;
            item.type = reply->element[i]->type;
            item.str.assign(reply->element[i]->str, reply->element[i]->len);
            array.push_back(item);
        }
        bRet = true;
    } else {
        SetErrInfo(index, reply);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);
    return bRet;
}

bool xRedisClient::commandargv_array_ex(const SliceIndex& index, const VDATA& vDataIn, xRedisContext& ctx) {
    bool bRet = false;
    RedisConn* pRedisConn = mRedisPool->GetConnection(index.mNodeIndex, index.mSliceIndex, index.mIOtype);
    if (NULL == pRedisConn) {
        SetErrString(index, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    vector<const char*> argv(vDataIn.size());
    vector<size_t> argvlen(vDataIn.size());
    uint32_t j = 0;
    for (VDATA::const_iterator i = vDataIn.begin(); i != vDataIn.end(); ++i, ++j) {
        argv[j] = i->c_str(), argvlen[j] = i->size();
    }

    redisReply* reply = static_cast<redisReply*>(redisCommandArgv(pRedisConn->getCtx(), static_cast<int32_t>(argv.size()), &(argv[0]), &(argvlen[0])));
    if (RedisPool::CheckReply(reply))
        bRet = true;
    else
        SetErrInfo(index, reply);

    RedisPool::FreeReply(reply);
    ctx.conn = pRedisConn;
    return bRet;
}

int32_t xRedisClient::GetReply(xRedisContext* ctx, ReplyData& vData) {
    //vData.clear();
    //ReplyData(vData).swap(vData);
    redisReply* reply;
    RedisConn* pRedisConn = static_cast<RedisConn*>(ctx->conn);
    int32_t ret = redisGetReply(pRedisConn->getCtx(), (void**) &reply);
    if (0 == ret) {
        for (size_t i = 0; i < reply->elements; i++) {
            DataItem item;
            item.type = reply->element[i]->type;
            item.str.assign(reply->element[i]->str, reply->element[i]->len);
            vData.push_back(item);
        }
    }

    RedisPool::FreeReply(reply);

    return ret;
}

bool xRedisClient::GetxRedisContext(SliceIndex& index, xRedisContext* ctx) {
    RedisConn* pRedisConn = mRedisPool->GetConnection(index.mNodeIndex, index.mSliceIndex, index.mIOtype);
    if (NULL == pRedisConn) {
        return false;
    }
    ctx->conn = pRedisConn;
    return true;
}

void xRedisClient::FreexRedisContext(xRedisContext* ctx) {
    RedisConn* pRedisConn = static_cast<RedisConn*>(ctx->conn);
    redisReply* reply = static_cast<redisReply*>(redisCommand(pRedisConn->getCtx(), "unsubscribe"));
    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection((RedisConn*) ctx->conn);
}

bool xRedisClient::commandargv_bool(const SliceIndex& index, const VDATA& vData) {
    bool bRet = false;
    RedisConn* pRedisConn = mRedisPool->GetConnection(index.mNodeIndex, index.mSliceIndex, index.mIOtype);
    if (NULL == pRedisConn) {
        SetErrString(index, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return bRet;
    }

    vector<const char*> argv(vData.size());
    vector<size_t> argvlen(vData.size());
    uint32_t j = 0;
    for (VDATA::const_iterator i = vData.begin(); i != vData.end(); ++i, ++j) {
        argv[j] = i->c_str(), argvlen[j] = i->size();
    }

    redisReply* reply = static_cast<redisReply*>(redisCommandArgv(pRedisConn->getCtx(), static_cast<int32_t>(argv.size()), &(argv[0]), &(argvlen[0])));
    if (RedisPool::CheckReply(reply))
        bRet = reply->integer == 1;
    else
        SetErrInfo(index, reply);

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);

    return bRet;
}

bool xRedisClient::commandargv_status(const SliceIndex& index, const VDATA& vData) {
    bool bRet = false;
    RedisConn* pRedisConn = mRedisPool->GetConnection(index.mNodeIndex, index.mSliceIndex, index.mIOtype);
    if (NULL == pRedisConn) {
        SetErrString(index, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return bRet;
    }

    vector<const char*> argv(vData.size());
    vector<size_t> argvlen(vData.size());
    uint32_t j = 0;
    for (VDATA::const_iterator i = vData.begin(); i != vData.end(); ++i, ++j) {
        argv[j] = i->c_str(), argvlen[j] = i->size();
    }

    redisReply* reply = static_cast<redisReply*>(redisCommandArgv(pRedisConn->getCtx(), static_cast<int32_t>(argv.size()), &(argv[0]), &(argvlen[0])));
    if (RedisPool::CheckReply(reply)) {
        // Assume good reply until further inspection
        bRet = true;

        if (REDIS_REPLY_STRING == reply->type) {
            if (!reply->len || !reply->str || strcasecmp(reply->str, "OK") != 0)
                bRet = false;
        }
    } else {
        SetErrInfo(index, reply);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);

    return bRet;
}

bool xRedisClient::commandargv_array(const SliceIndex& index, const VDATA& vDataIn, ArrayReply& array) {
    bool bRet = false;
    RedisConn* pRedisConn = mRedisPool->GetConnection(index.mNodeIndex, index.mSliceIndex, index.mIOtype);
    if (NULL == pRedisConn) {
        SetErrString(index, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    vector<const char*> argv(vDataIn.size());
    vector<size_t> argvlen(vDataIn.size());
    uint32_t j = 0;
    for (VDATA::const_iterator i = vDataIn.begin(); i != vDataIn.end(); ++i, ++j) {
        argv[j] = i->c_str(), argvlen[j] = i->size();
    }

    redisReply* reply = static_cast<redisReply*>(redisCommandArgv(pRedisConn->getCtx(), static_cast<int32_t>(argv.size()), &(argv[0]), &(argvlen[0])));
    if (RedisPool::CheckReply(reply)) {
        for (size_t i = 0; i < reply->elements; i++) {
            DataItem item;
            item.type = reply->element[i]->type;
            item.str.assign(reply->element[i]->str, reply->element[i]->len);
            array.push_back(item);
        }
        bRet = true;
    } else {
        SetErrInfo(index, reply);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);
    return bRet;
}

bool xRedisClient::commandargv_array(const SliceIndex& index, const VDATA& vDataIn, VALUES& array) {
    bool bRet = false;
    RedisConn* pRedisConn = mRedisPool->GetConnection(index.mNodeIndex, index.mSliceIndex, index.mIOtype);
    if (NULL == pRedisConn) {
        SetErrString(index, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    vector<const char*> argv(vDataIn.size());
    vector<size_t> argvlen(vDataIn.size());
    uint32_t j = 0;
    for (VDATA::const_iterator i = vDataIn.begin(); i != vDataIn.end(); ++i, ++j) {
        argv[j] = i->c_str(), argvlen[j] = i->size();
    }

    redisReply* reply = static_cast<redisReply*>(redisCommandArgv(pRedisConn->getCtx(), static_cast<int32_t>(argv.size()), &(argv[0]), &(argvlen[0])));
    if (RedisPool::CheckReply(reply)) {
        for (size_t i = 0; i < reply->elements; i++) {
            string str(reply->element[i]->str, reply->element[i]->len);
            array.push_back(str);
        }
        bRet = true;
    } else {
        SetErrInfo(index, reply);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);
    return bRet;
}

bool xRedisClient::commandargv_integer(const SliceIndex& index, const VDATA& vDataIn, int64_t& retval) {
    bool bRet = false;
    RedisConn* pRedisConn = mRedisPool->GetConnection(index.mNodeIndex, index.mSliceIndex, index.mIOtype);
    if (NULL == pRedisConn) {
        SetErrString(index, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    vector<const char*> argv(vDataIn.size());
    vector<size_t> argvlen(vDataIn.size());
    uint32_t j = 0;
    for (VDATA::const_iterator iter = vDataIn.begin(); iter != vDataIn.end(); ++iter, ++j) {
        argv[j] = iter->c_str(), argvlen[j] = iter->size();
    }

    redisReply* reply = static_cast<redisReply*>(redisCommandArgv(pRedisConn->getCtx(), static_cast<int32_t>(argv.size()), &(argv[0]), &(argvlen[0])));
    if (RedisPool::CheckReply(reply)) {
        retval = reply->integer;
        bRet = true;
    } else {
        SetErrInfo(index, reply);
    }

    RedisPool::FreeReply(reply);
    mRedisPool->FreeConnection(pRedisConn);
    return bRet;
}

bool xRedisClient::ScanFun(const char* cmd, const SliceIndex& index, const std::string* key,
                           int64_t& cursor, const char* pattern, uint32_t count, ArrayReply& array, xRedisContext& ctx) {
    SETDEFAULTIOTYPE(MASTER)
    VDATA vCmdData;
    vCmdData.push_back(cmd);
    if (NULL != key) {
        vCmdData.push_back(*key);
    }

    vCmdData.push_back(toString(cursor));

    if (NULL != pattern) {
        vCmdData.push_back("MATCH");
        vCmdData.push_back(pattern);
    }

    if (0 != count) {
        vCmdData.push_back("COUNT");
        vCmdData.push_back(toString(count));
    }

    bool bRet = false;
    RedisConn* pRedisConn = static_cast<RedisConn*>(ctx.conn);
    if (NULL == pRedisConn) {
        SetErrString(index, GET_CONNECT_ERROR, ::strlen(GET_CONNECT_ERROR));
        return false;
    }

    vector<const char*> argv(vCmdData.size());
    vector<size_t> argvLen(vCmdData.size());
    uint32_t j = 0;
    for (VDATA::const_iterator i = vCmdData.begin(); i != vCmdData.end(); ++i, ++j) {
        argv[j] = i->c_str(), argvLen[j] = i->size();
    }

    redisReply* reply = static_cast<redisReply*>(redisCommandArgv(pRedisConn->getCtx(), static_cast<int32_t>(argv.size()), &(argv[0]), &(argvLen[0])));
    if (RedisPool::CheckReply(reply)) {
        if (0 == reply->elements) {
            cursor = 0;
        } else {
            cursor = atoi(reply->element[0]->str);
            redisReply** replyData = reply->element[1]->element;
            for (size_t i = 0; i < reply->element[1]->elements; i++) {
                DataItem item;
                item.type = replyData[i]->type;
                item.str.assign(replyData[i]->str, replyData[i]->len);
                array.push_back(item);
            }
        }
        bRet = true;
    } else {
        SetErrInfo(index, reply);
    }
    RedisPool::FreeReply(reply);
    return bRet;
}
