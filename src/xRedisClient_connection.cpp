/*
 * ----------------------------------------------------------------------------
 * Copyright (c) 2013-2014, Leiwenbin
 * All rights reserved.
 * Distributed under GPL license.
 * ----------------------------------------------------------------------------
 */

#include <redis/xredis/xRedisClient.h>

using namespace xrcp;

void xRedisClient::quit() {
    Release();
}


bool xRedisClient::echo(const SliceIndex& index, const string& str, std::string& value) {
    if (0 == str.length()) return false;
    SETDEFAULTIOTYPE(MASTER)
    return command_string(index, value, "echo %s", str.c_str());
}

bool xRedisClient::select(const SliceIndex& index, int32_t pos) {
    if (pos < 1) return false;
    SETDEFAULTIOTYPE(MASTER)
    return command_bool(index, "SELECT %d", pos);
}














