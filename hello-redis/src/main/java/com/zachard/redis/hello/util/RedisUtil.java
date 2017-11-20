/*
 *  Copyright 2015-2017 zachard, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.zachard.redis.hello.util;

import java.util.Collections;
import java.util.Objects;

import redis.clients.jedis.Jedis;

/**
 * Redis工具类
 * <pre>
 *     为了确保分布式锁可用, 至少要确保锁的实现满足以下四个条件:
 *     (1) 互斥性: 在任意时刻, 只有一个客户端能持有锁
 *     (2) 不会发生死锁: 即使有一个客户端在持有锁的期间崩溃而没有主动解锁, 也能保证后续其他客户端能加锁
 *                     一般通过设置锁超时时间来实现
 *     (3) 具有容错性: 只要大部分的<code>Redis</code>节点正常运行, 客户端就可以加锁和解锁
 *     (4) 解铃还须系铃人: 加锁和解锁必须是同一个客户端, 客户端自己不能把别人加的锁给解了
 * </pre>
 *
 * @author zachard
 * @version 1.0.0
 */
public class RedisUtil {
	
	/**
	 * 加锁成功响应码
	 */
	private static final String LOCK_SUCESS = "OK";
	
	/**
	 * <code>setnx</code>命令参数
	 */
	private static final String SET_IF_NOT_EXIST = "NX";
	
	/**
	 * <code>expire</code>命令参数
	 */
	private static final String SET_WITH_EPIRE_TIME = "PX";
	
	/**
	 * 锁释放成功响应码
	 */
	private static final Long UNLOCK_SUCCESS = 1L;
	
	/**
	 * 删除<code>redis</code>键值的<code>Lua</code>脚本
	 */
	private static final String UNLOCK_SCRIPT = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
	
	/**
	 * 加<code>redis</code>分布式锁
	 * 
	 * @param jedis       {@link Jedis}对象
	 * @param lockKey     锁的key
	 * @param requestId   锁的值,用于唯一标识加锁客户端,可用{@link UUID#randomUUID()#toString()}方法生成
	 * @param expireTime  加锁时长
	 * @return            加锁成功,返回<code>true</code>, 否则返回<code>false</code>
	 */
	public static boolean lock(Jedis jedis, String lockKey, String requestId, int expireTime) {
		String result = jedis.set(lockKey, requestId, SET_IF_NOT_EXIST, 
				SET_WITH_EPIRE_TIME, expireTime);
		
		if (Objects.equals(LOCK_SUCESS, result)) {
			return true;
		}
		
		return false;
	}
	
	/**
	 * 释放<code>redis</code>分布式锁
	 * 
	 * @param jedis       {@link Jedis}对象
	 * @param lockKey     分布式锁的Key
	 * @param requestId   锁的值, 用于标识加锁的客户端
	 * @return            锁释放成功,返回<code>true</code>, 否则返回<code>false</code>
	 */
	public static boolean unlock(Jedis jedis, String lockKey, String requestId) {
		Object result = jedis.eval(UNLOCK_SCRIPT, Collections.singletonList(lockKey), 
				Collections.singletonList(requestId));
		
		if (Objects.equals(result, UNLOCK_SUCCESS)) {
			return true;
		}
		
		return false;
	}
}
