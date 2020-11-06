/*
 * ----------------------------------------------------------------------------
 * Copyright (c) 2013-2014, Leiwenbin
 * All rights reserved.
 * Distributed under GPL license.
 * ----------------------------------------------------------------------------
 */

#include <redis/xredis/xRedisPool.h>

using namespace xrcp;

RedisPool::RedisPool() {
    mRedisCacheNodeList = NULL;
    mNodeCount = 0;
    srand((unsigned) time(NULL));
}

RedisPool::~RedisPool() {

}

bool RedisPool::Init(uint32_t nodeCount) {
    mNodeCount = nodeCount;
    if (mNodeCount > MAX_REDIS_NODE_COUNT) {
        return false;
    }

    mRedisCacheNodeList = new RedisCacheNode[mNodeCount];
    return mRedisCacheNodeList != NULL;
}

bool RedisPool::SetSliceCount(uint32_t nodeIndex, uint32_t sliceCount) {
    if ((sliceCount > MAX_REDIS_SLICE_COUNT) || (nodeIndex > mNodeCount - 1)) {
        return false;
    }
    bool bRet = mRedisCacheNodeList[nodeIndex].InitDB(nodeIndex, sliceCount);
    return bRet;
}

uint32_t RedisPool::GetSliceCount(uint32_t nodeIndex) {
    if ((nodeIndex > mNodeCount) || (nodeIndex > MAX_REDIS_NODE_COUNT)) {
        return 0;
    }
    return mRedisCacheNodeList[nodeIndex].GetSliceCount();
}

void RedisPool::Keepalive() {
    for (uint32_t i = 0; i < mNodeCount; i++) {
        if (mRedisCacheNodeList[i].GetSliceCount() > 0) {
            mRedisCacheNodeList[i].KeepAlive();
        }
    }
}

bool RedisPool::CheckReply(const redisReply* reply) {
    if (NULL == reply)
        return false;

    switch (reply->type) {
        case REDIS_REPLY_STRING:
            return true;
        case REDIS_REPLY_ARRAY:
            return true;
        case REDIS_REPLY_INTEGER:
            return true;
        case REDIS_REPLY_NIL:
            return false;
        case REDIS_REPLY_STATUS:
            return true;
        case REDIS_REPLY_ERROR:
            return false;
        default:
            return false;
    }
}

void RedisPool::FreeReply(const redisReply* reply) {
    if (NULL != reply)
        freeReplyObject((void*) reply);
}

bool RedisPool::ConnectRedisDB(uint32_t nodeIndex, uint32_t sliceIndex, const string& host, uint32_t port, const string& passwd, uint32_t poolSize, uint32_t timeout, uint32_t role) {
    if ((0 == host.length()) || (nodeIndex > MAX_REDIS_NODE_COUNT - 1) || (sliceIndex > MAX_REDIS_SLICE_COUNT - 1) || (role > SLAVE) || (poolSize > MAX_REDIS_CONN_POOL_COUNT))
        return false;

    return mRedisCacheNodeList[nodeIndex].ConnectRedisDB(nodeIndex, sliceIndex, host, port, passwd, poolSize, timeout, role);
}

void RedisPool::Release() {
    for (uint32_t i = 0; i < mNodeCount; i++) {
        if (mRedisCacheNodeList[i].GetSliceCount() > 0)
            mRedisCacheNodeList[i].ClosePool();
    }
    delete[] mRedisCacheNodeList;
}

uint32_t RedisPool::GetNodeCount() {
    return mNodeCount;
}

RedisConn* RedisPool::GetConnection(uint32_t nodeIndex, uint32_t sliceIndex, uint32_t ioType) {
    RedisConn* pRedisConn = NULL;

    if ((nodeIndex > mNodeCount) || (sliceIndex > mRedisCacheNodeList[nodeIndex].GetSliceCount()) || (ioType > SLAVE))
        return NULL;

    RedisCacheNode* pRedisCacheNode = &mRedisCacheNodeList[nodeIndex];
    pRedisConn = pRedisCacheNode->GetConn(sliceIndex, ioType);

    return pRedisConn;
}

void RedisPool::FreeConnection(RedisConn* redisconn) {
    if (NULL != redisconn)
        mRedisCacheNodeList[redisconn->GetNodeIndex()].FreeConn(redisconn);
}

RedisConn::RedisConn() {
    mCtx = NULL;
    mPort = 0;
    mTimeout = 0;
    mPoolsize = 0;
    mNodeIndex = 0;
    mSliceIndex = 0;
    mConnStatus = false;
}

RedisConn::~RedisConn() {

}

redisContext* RedisConn::ConnectWithTimeout() {
    struct timeval timeoutVal;
    timeoutVal.tv_sec = mTimeout;
    timeoutVal.tv_usec = 0;

    redisContext* ctx = NULL;
    ctx = redisConnectWithTimeout(mHost.c_str(), mPort, timeoutVal);
    if (NULL == ctx || ctx->err) {
        if (NULL != ctx) {
            redisFree(ctx);
            ctx = NULL;
        }
    }

    return ctx;
}

bool RedisConn::auth() {
    bool bRet;
    if (0 == mPass.length()) {
        bRet = true;
    } else {
        redisReply* reply = static_cast<redisReply*>(redisCommand(mCtx, "AUTH %s", mPass.c_str()));
        bRet = !((NULL == reply) || (strcasecmp(reply->str, "OK") != 0));
        freeReplyObject(reply);
    }

    return bRet;
}

bool RedisConn::RedisConnect() {
    bool bRet;
    if (NULL != mCtx) {
        redisFree(mCtx);
        mCtx = NULL;
    }

    mCtx = ConnectWithTimeout();
    if (NULL == mCtx) {
        bRet = false;
    } else {
        bRet = auth();
        mConnStatus = bRet;
    }

    return bRet;
}

bool RedisConn::RedisReConnect() {
    if (NULL == mCtx)
        return false;

    bool bRet;
    redisContext* tmp_ctx = ConnectWithTimeout();
    if (NULL == tmp_ctx) {
        bRet = false;
    } else {
        redisFree(mCtx);
        mCtx = tmp_ctx;
        bRet = auth();
    }

    mConnStatus = bRet;
    return bRet;
}

bool RedisConn::Ping() {
    redisReply* reply = static_cast<redisReply*>(redisCommand(mCtx, "PING"));
    bool bRet = (NULL != reply);
    mConnStatus = bRet;
    if (bRet)
        freeReplyObject(reply);
    return bRet;
}

void RedisConn::Init(uint32_t nodeIndex, uint32_t sliceIndex, const std::string& host, uint32_t port, const std::string& pass, uint32_t poolSize, uint32_t timeout, uint32_t role, uint32_t slaveidx) {
    mNodeIndex = nodeIndex;
    mSliceIndex = sliceIndex;
    mHost = host;
    mPort = port;
    mPass = pass;
    mPoolsize = poolSize;
    mTimeout = timeout;
    mRole = role;
    mSlaveIdx = slaveidx;
}

RedisDBSlice::RedisDBSlice() {
    mNodeIndex = 0;
    mSliceIndex = 0;
    mStatus = 0;
    mHaveSlave = false;
}

RedisDBSlice::~RedisDBSlice() {

}

void RedisDBSlice::Init(uint32_t nodeIndex, uint32_t sliceIndex) {
    mNodeIndex = nodeIndex;
    mSliceIndex = sliceIndex;
}

bool RedisDBSlice::ConnectRedisNodes(uint32_t nodeIndex, uint32_t sliceIndex, const string& host, uint32_t port, const string& passwd, uint32_t poolsize, uint32_t timeout, uint32_t role) {
    bool bRet = false;
    if ((host.empty()) || (nodeIndex > MAX_REDIS_NODE_COUNT) || (sliceIndex > MAX_REDIS_SLICE_COUNT) || (poolsize > MAX_REDIS_CONN_POOL_COUNT))
        return false;

    try {
        if (MASTER == role) {
            XLOCK(mSliceConn.MasterLock);
            for (uint32_t i = 0; i < poolsize; ++i) {
                RedisConn* pRedisconn = new RedisConn;

                pRedisconn->Init(nodeIndex, sliceIndex, host.c_str(), port, passwd.c_str(), poolsize, timeout, role, 0);
                if (pRedisconn->RedisConnect()) {
                    mSliceConn.RedisMasterConn.push_back(pRedisconn);
                    mStatus = REDISDB_WORKING;
                    bRet = true;
                } else {
                    delete pRedisconn;
                }
            }
        } else if (SLAVE == role) {
            XLOCK(mSliceConn.SlaveLock);
            RedisConnPool* pSlaveNode = new RedisConnPool;
            uint32_t slave_idx = (int32_t) mSliceConn.RedisSlaveConn.size();
            for (uint32_t i = 0; i < poolsize; ++i) {
                RedisConn* pRedisconn = new RedisConn;

                pRedisconn->Init(nodeIndex, sliceIndex, host.c_str(), port, passwd.c_str(), poolsize, timeout, role, slave_idx);
                if (pRedisconn->RedisConnect()) {
                    pSlaveNode->push_back(pRedisconn);
                    bRet = true;
                } else {
                    delete pRedisconn;
                }
            }
            mSliceConn.RedisSlaveConn.push_back(pSlaveNode);
            mHaveSlave = true;
        } else {
            bRet = false;
        }

    } catch (...) {
        return false;
    }

    return bRet;
}

RedisConn* RedisDBSlice::GetMasterConn() {
    RedisConn* pRedisConn = NULL;
    XLOCK(mSliceConn.MasterLock);
    if (!mSliceConn.RedisMasterConn.empty()) {
        pRedisConn = mSliceConn.RedisMasterConn.front();
        mSliceConn.RedisMasterConn.pop_front();
    } else {
        mStatus = REDISDB_DEAD;
    }
    return pRedisConn;
}

RedisConn* RedisDBSlice::GetSlaveConn() {
    RedisConn* pRedisConn = NULL;
    XLOCK(mSliceConn.SlaveLock);
    if (!mSliceConn.RedisSlaveConn.empty()) {
        size_t slave_cnt = mSliceConn.RedisSlaveConn.size();
        uint32_t idx = (uint32_t) (rand() % slave_cnt);
        RedisConnPool* pSlave = mSliceConn.RedisSlaveConn[idx];
        pRedisConn = pSlave->front();
        pSlave->pop_front();
    }
    return pRedisConn;
}

RedisConn* RedisDBSlice::GetConn(int32_t ioRole) {
    RedisConn* pRedisConn = NULL;
    if (!mHaveSlave)
        ioRole = MASTER;
    if (MASTER == ioRole)
        pRedisConn = GetMasterConn();
    else if (SLAVE == ioRole)
        pRedisConn = GetSlaveConn();

    return pRedisConn;
}

void RedisDBSlice::FreeConn(RedisConn* redisconn) {
    if (NULL != redisconn) {
        uint32_t role = redisconn->GetRole();
        if (MASTER == role) {
            XLOCK(mSliceConn.MasterLock);
            mSliceConn.RedisMasterConn.push_back(redisconn);
        } else if (SLAVE == role) {
            XLOCK(mSliceConn.SlaveLock);
            RedisConnPool* pSlave = mSliceConn.RedisSlaveConn[redisconn->GetSlaveIdx()];
            pSlave->push_back(redisconn);
        } else {

        }
    }
}

void RedisDBSlice::CloseConnPool() {
    {
        XLOCK(mSliceConn.MasterLock);
        RedisConnIter master_iter = mSliceConn.RedisMasterConn.begin();
        for (; master_iter != mSliceConn.RedisMasterConn.end(); ++master_iter) {
            redisFree((*master_iter)->getCtx());
            delete *master_iter;
        }
    }

    {
        XLOCK(mSliceConn.SlaveLock);
        RedisSlaveGroupIter slave_iter = mSliceConn.RedisSlaveConn.begin();
        for (; slave_iter != mSliceConn.RedisSlaveConn.end(); ++slave_iter) {
            RedisConnPool* pConnPool = (*slave_iter);
            RedisConnIter iter = pConnPool->begin();
            for (; iter != pConnPool->end(); ++iter) {
                redisFree((*iter)->getCtx());
                delete *iter;
            }
            delete pConnPool;
        }
    }

    mStatus = REDISDB_DEAD;
}

void RedisDBSlice::ConnPoolPing() {
    {
        XLOCK(mSliceConn.MasterLock);
        RedisConnIter master_iter = mSliceConn.RedisMasterConn.begin();
        for (; master_iter != mSliceConn.RedisMasterConn.end(); ++master_iter) {
            bool bRet = (*master_iter)->Ping();
            if (!bRet)
                (*master_iter)->RedisReConnect();
        }
    }

    {
        XLOCK(mSliceConn.SlaveLock);
        RedisSlaveGroupIter slave_iter = mSliceConn.RedisSlaveConn.begin();
        for (; slave_iter != mSliceConn.RedisSlaveConn.end(); ++slave_iter) {
            RedisConnPool* pConnPool = (*slave_iter);
            RedisConnIter iter = pConnPool->begin();
            for (; iter != pConnPool->end(); ++iter) {
                bool bRet = (*iter)->Ping();
                if (!bRet)
                    (*iter)->RedisReConnect();

            }
        }
    }
}

uint32_t RedisDBSlice::GetStatus() const {
    return mStatus;
}

RedisCacheNode::RedisCacheNode() {
    mNodeIndex = 0;
    mSliceCount = 0;
    mRedisDBSliceList = NULL;
}

RedisCacheNode::~RedisCacheNode() {

}

bool RedisCacheNode::InitDB(uint32_t nodeIndex, uint32_t sliceCount) {
    mNodeIndex = nodeIndex;
    mSliceCount = sliceCount;
    if (NULL == mRedisDBSliceList)
        mRedisDBSliceList = new RedisDBSlice[sliceCount];

    return true;
}

bool RedisCacheNode::ConnectRedisDB(uint32_t nodeIndex, uint32_t sliceIndex, const string& host, uint32_t port, const string& passwd, uint32_t poolSize, uint32_t timeout, uint32_t role) {
    mRedisDBSliceList[sliceIndex].Init(nodeIndex, sliceIndex);
    return mRedisDBSliceList[sliceIndex].ConnectRedisNodes(nodeIndex, sliceIndex, host, port, passwd, poolSize, timeout, role);
}

void RedisCacheNode::ClosePool() {
    for (uint32_t i = 0; i < mSliceCount; i++) {
        mRedisDBSliceList[i].CloseConnPool();
    }
    delete[] mRedisDBSliceList;
    mRedisDBSliceList = NULL;
}

void RedisCacheNode::KeepAlive() {
    for (uint32_t i = 0; i < mSliceCount; i++) {
        mRedisDBSliceList[i].ConnPoolPing();
    }
}

uint32_t RedisCacheNode::GetSliceStatus(uint32_t sliceIndex) {
    RedisDBSlice* pdbSlice = &mRedisDBSliceList[sliceIndex];
    if (NULL == pdbSlice)
        return REDISDB_UNCONN;
    return pdbSlice->GetStatus();
}

void RedisCacheNode::FreeConn(RedisConn* redisconn) {
    return mRedisDBSliceList[redisconn->getSliceIndex()].FreeConn(redisconn);
}

RedisConn* RedisCacheNode::GetConn(uint32_t sliceIndex, uint32_t ioRole) {
    return mRedisDBSliceList[sliceIndex].GetConn(ioRole);
}

uint32_t RedisCacheNode::GetSliceCount() const {
    return mSliceCount;
}


