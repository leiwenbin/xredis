/*
 * ----------------------------------------------------------------------------
 * Copyright (c) 2013-2014, xSky <guozhw at gmail dot com>
 * All rights reserved.
 * Distributed under GPL license.
 * ----------------------------------------------------------------------------
 */

#include <redis/xredis/xRedisClient.h>

void xRedisClient::quit() {
    Release();
}


bool xRedisClient::echo(const RedisDBIdx& dbi, const string& str, std::string& value) {
    if (0 == str.length())
        return false;
    SETDEFAULTIOTYPE(MASTER);
    return command_string(dbi, value, "echo %s", str.c_str());
}

bool xRedisClient::select(const RedisDBIdx& dbi, int pos) {
    if (pos < 1)
        return false;
    SETDEFAULTIOTYPE(MASTER);
    return command_bool(dbi, "SELECT %d", pos);
}














