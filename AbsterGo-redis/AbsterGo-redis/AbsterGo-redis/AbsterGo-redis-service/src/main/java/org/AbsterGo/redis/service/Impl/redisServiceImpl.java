package org.AbsterGo.redis.service.Impl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.AbsterGo.redis.service.BasicJedisService;
import org.AbsterGo.redis.service.redisService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.dubbo.config.annotation.Service;
import com.alibaba.fastjson.JSON;

import Com.AbsterGo.redis.api.SerializeUtil;
import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;

@Service
public class redisServiceImpl extends BasicJedisService implements redisService{

	private static final Logger log = LoggerFactory.getLogger(redisServiceImpl.class);

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.service.redisService#disconnect()
	 * 
	 * 失联
	 */
	public void disconnect() {
		getJedisCon().disconnect();

	}


	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.service.redisService#set(java.lang.String,
	 * java.lang.String) 加入字符串
	 */
	public String set(String key, String value) {

		String result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.set(key, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public String setObject(String key, Object value) {

		String result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.set(key, SerializeUtil.serialize(value));

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#get(java.lang.String)
	 * 按照key获取
	 */

	public String get(String key) {
		String result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.get(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#exists(java.lang.String)
	 * 检验是否存在
	 */
	public Boolean exists(String key) {
		Boolean result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.exists(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#type(java.lang.String)
	 * 返回key的type
	 */

	public String type(String key) {
		String result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.type(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#expire(java.lang.String,
	 * int) 设置存活时间
	 */
	public Long expire(String key, int seconds) {

		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.expire(key, seconds);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.AbsterGo.redis.serviceImpl.redisService#expireAt(java.lang.String,
	 * long) 设置存活时间（时间戳）
	 */
	public Long expireAt(String key, long unixTime) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.expireAt(key, unixTime);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#ttl(java.lang.String)
	 * 查看时间戳
	 */

	public Long ttl(String key) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.ttl(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#setbit(java.lang.String,
	 * long, boolean)
	 * 
	 */
	public boolean setbit(String key, long offset, boolean value) {
		Boolean result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.setbit(key, offset, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#getbit(java.lang.String,
	 * long)
	 */
	public boolean getbit(String key, long offset) {
		Boolean result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.getbit(key, offset);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.AbsterGo.redis.serviceImpl.redisService#setrange(java.lang.String,
	 * long, java.lang.String)
	 */
	public long setrange(String key, long offset, String value) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.setrange(key, offset, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.AbsterGo.redis.serviceImpl.redisService#getrange(java.lang.String,
	 * long, long)
	 */
	public String getrange(String key, long startOffset, long endOffset) {
		String result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.getrange(key, startOffset, endOffset);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#getSet(java.lang.String,
	 * java.lang.String)
	 */
	public String getSet(String key, String value) {
		String result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.getSet(key, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#setnx(java.lang.String,
	 * java.lang.String)
	 */
	public Long setnx(String key, String value) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.setnx(key, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#setex(java.lang.String,
	 * int, java.lang.String)
	 */
	public String setex(String key, int seconds, String value) {
		String result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.setex(key, seconds, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#decrBy(java.lang.String,
	 * long)
	 */
	public Long decrBy(String key, long integer) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.decrBy(key, integer);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#decr(java.lang.String)
	 */
	public Long decr(String key) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.decr(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#incrBy(java.lang.String,
	 * long)
	 */
	public Long incrBy(String key, long integer) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.incrBy(key, integer);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#incr(java.lang.String)
	 */
	public Long incr(String key) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.incr(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#append(java.lang.String,
	 * java.lang.String)
	 */
	public Long append(String key, String value) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.append(key, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#substr(java.lang.String,
	 * int, int)
	 */
	public String substr(String key, int start, int end) {
		String result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.substr(key, start, end);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#hset(java.lang.String,
	 * java.lang.String, java.lang.String)
	 */
	public Long hset(String key, String field, String value) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.hset(key, field, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#hget(java.lang.String,
	 * java.lang.String)
	 */
	public String hget(String key, String field) {
		String result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.hget(key, field);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#hsetnx(java.lang.String,
	 * java.lang.String, java.lang.String)
	 */
	public Long hsetnx(String key, String field, String value) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.hsetnx(key, field, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public String hmset(String key, Map<String, String> hash) {
		String result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.hmset(key, hash);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public List<String> hmget(String key, String... fields) {
		List<String> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.hmget(key, fields);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.AbsterGo.redis.serviceImpl.redisService#hincrBy(java.lang.String,
	 * java.lang.String, long)
	 */
	public Long hincrBy(String key, String field, long value) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.hincrBy(key, field, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.AbsterGo.redis.serviceImpl.redisService#hexists(java.lang.String,
	 * java.lang.String)
	 */
	public Boolean hexists(String key, String field) {
		Boolean result = false;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.hexists(key, field);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#del(java.lang.String)
	 */
	public Long del(String key) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.del(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#hdel(java.lang.String,
	 * java.lang.String)
	 */
	public Long hdel(String key, String field) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.hdel(key, field);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#hlen(java.lang.String)
	 */
	public Long hlen(String key) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.hlen(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Set<String> hkeys(String key) {
		Set<String> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.hkeys(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public List<String> hvals(String key) {
		List<String> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.hvals(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Map<String, String> hgetAll(String key) {
		Map<String, String> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.hgetAll(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	// ================list ====== l表示 list或 left, r表示right====================
	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#rpush(java.lang.String,
	 * java.lang.String)
	 */
	public Long rpush(String key, String string) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.rpush(key, string);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#lpush(java.lang.String,
	 * java.lang.String)
	 */
	public Long lpush(String key, String string) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.lpush(key, string);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#llen(java.lang.String)
	 */
	public Long llen(String key) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.llen(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public List<String> lrange(String key, long start, long end) {
		List<String> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.lrange(key, start, end);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#ltrim(java.lang.String,
	 * long, long)
	 */
	public String ltrim(String key, long start, long end) {
		String result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.ltrim(key, start, end);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#lindex(java.lang.String,
	 * long)
	 */
	public String lindex(String key, long index) {
		String result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.lindex(key, index);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#lset(java.lang.String,
	 * long, java.lang.String)
	 */
	public String lset(String key, long index, String value) {
		String result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.lset(key, index, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#lrem(java.lang.String,
	 * long, java.lang.String)
	 */
	public Long lrem(String key, long count, String value) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.lrem(key, count, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#lpop(java.lang.String)
	 */
	public String lpop(String key) {
		String result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.lpop(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#rpop(java.lang.String)
	 */
	public String rpop(String key) {
		String result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.rpop(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	// return 1 add a not exist value ,
	// return 0 add a exist value
	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#sadd(java.lang.String,
	 * java.lang.String)
	 */
	public Long sadd(String key, String member) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.sadd(key, member);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Set<String> smembers(String key) {
		Set<String> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.smembers(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#srem(java.lang.String,
	 * java.lang.String)
	 */
	public Long srem(String key, String member) {
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();

		Long result = null;
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.srem(key, member);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#spop(java.lang.String)
	 */
	public String spop(String key) {
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		String result = null;
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.spop(key);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#scard(java.lang.String)
	 */
	public Long scard(String key) {
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		Long result = null;
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.scard(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.AbsterGo.redis.serviceImpl.redisService#sismember(java.lang.String,
	 * java.lang.String)
	 */
	public Boolean sismember(String key, String member) {
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		Boolean result = null;
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.sismember(key, member);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.AbsterGo.redis.serviceImpl.redisService#srandmember(java.lang.String)
	 */
	public String srandmember(String key) {
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		String result = null;
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.srandmember(key);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#zadd(java.lang.String,
	 * double, java.lang.String)
	 */
	public Long zadd(String key, double score, String member) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.zadd(key, score, member);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Set<String> zrange(String key, int start, int end) {
		Set<String> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.zrange(key, start, end);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#zrem(java.lang.String,
	 * java.lang.String)
	 */
	public Long zrem(String key, String member) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.zrem(key, member);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.AbsterGo.redis.serviceImpl.redisService#zincrby(java.lang.String,
	 * double, java.lang.String)
	 */
	public Double zincrby(String key, double score, String member) {
		Double result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zincrby(key, score, member);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#zrank(java.lang.String,
	 * java.lang.String)
	 */
	public Long zrank(String key, String member) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zrank(key, member);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.AbsterGo.redis.serviceImpl.redisService#zrevrank(java.lang.String,
	 * java.lang.String)
	 */
	public Long zrevrank(String key, String member) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zrevrank(key, member);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Set<String> zrevrange(String key, int start, int end) {
		Set<String> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zrevrange(key, start, end);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Set<Tuple> zrangeWithScores(String key, int start, int end) {
		Set<Tuple> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zrangeWithScores(key, start, end);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Set<Tuple> zrevrangeWithScores(String key, int start, int end) {
		Set<Tuple> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zrevrangeWithScores(key, start, end);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#zcard(java.lang.String)
	 */
	public Long zcard(String key) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zcard(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#zscore(java.lang.String,
	 * java.lang.String)
	 */
	public Double zscore(String key, String member) {
		Double result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zscore(key, member);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public List<String> sort(String key) {
		List<String> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.sort(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public List<String> sort(String key, SortingParams sortingParameters) {
		List<String> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.sort(key, sortingParameters);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#zcount(java.lang.String,
	 * double, double)
	 */
	public Long zcount(String key, double min, double max) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zcount(key, min, max);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Set<String> zrangeByScore(String key, double min, double max) {
		Set<String> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zrangeByScore(key, min, max);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Set<String> zrevrangeByScore(String key, double max, double min) {
		Set<String> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zrevrangeByScore(key, max, min);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
		Set<String> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zrangeByScore(key, min, max, offset, count);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
		Set<String> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zrevrangeByScore(key, max, min, offset, count);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
		Set<Tuple> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zrangeByScoreWithScores(key, min, max);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
		Set<Tuple> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zrevrangeByScoreWithScores(key, max, min);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
		Set<Tuple> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zrangeByScoreWithScores(key, min, max, offset, count);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
		Set<Tuple> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zrevrangeByScoreWithScores(key, max, min, offset, count);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.AbsterGo.redis.serviceImpl.redisService#zremrangeByRank(java.lang.
	 * String, int, int)
	 */
	public Long zremrangeByRank(String key, int start, int end) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zremrangeByRank(key, start, end);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.AbsterGo.redis.serviceImpl.redisService#zremrangeByScore(java.lang.
	 * String, double, double)
	 */
	public Long zremrangeByScore(String key, double start, double end) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zremrangeByScore(key, start, end);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Long linsert(String key, LIST_POSITION where, String pivot, String value) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.linsert(key, where, pivot, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#set(byte[], byte[])
	 */
	public String set(byte[] key, byte[] value) {
		String result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.set(key, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#get(byte[])
	 */
	public byte[] get(byte[] key) {
		byte[] result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.get(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#exists(byte[])
	 */
	public Boolean exists(byte[] key) {
		Boolean result = false;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.exists(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#type(byte[])
	 */
	public String type(byte[] key) {
		String result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.type(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#expire(byte[], int)
	 */
	public Long expire(byte[] key, int seconds) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.expire(key, seconds);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#expireAt(byte[], long)
	 */
	public Long expireAt(byte[] key, long unixTime) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.expireAt(key, unixTime);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#ttl(byte[])
	 */
	public Long ttl(byte[] key) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.ttl(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#getSet(byte[], byte[])
	 */
	public byte[] getSet(byte[] key, byte[] value) {
		byte[] result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.getSet(key, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#setnx(byte[], byte[])
	 */
	public Long setnx(byte[] key, byte[] value) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.setnx(key, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#setex(byte[], int,
	 * byte[])
	 */
	public String setex(byte[] key, int seconds, byte[] value) {
		String result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.setex(key, seconds, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#decrBy(byte[], long)
	 */
	public Long decrBy(byte[] key, long integer) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.decrBy(key, integer);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#decr(byte[])
	 */
	public Long decr(byte[] key) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.decr(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#incrBy(byte[], long)
	 */
	public Long incrBy(byte[] key, long integer) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.incrBy(key, integer);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#incr(byte[])
	 */
	public Long incr(byte[] key) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.incr(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#append(byte[], byte[])
	 */
	public Long append(byte[] key, byte[] value) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.append(key, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#substr(byte[], int, int)
	 */
	public byte[] substr(byte[] key, int start, int end) {
		byte[] result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.substr(key, start, end);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#hset(byte[], byte[],
	 * byte[])
	 */
	public Long hset(byte[] key, byte[] field, byte[] value) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.hset(key, field, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#hget(byte[], byte[])
	 */
	public byte[] hget(byte[] key, byte[] field) {
		byte[] result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.hget(key, field);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#hsetnx(byte[], byte[],
	 * byte[])
	 */
	public Long hsetnx(byte[] key, byte[] field, byte[] value) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.hsetnx(key, field, value);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public String hmset(byte[] key, Map<byte[], byte[]> hash) {
		String result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.hmset(key, hash);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public List<byte[]> hmget(byte[] key, byte[]... fields) {
		List<byte[]> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.hmget(key, fields);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#hincrBy(byte[], byte[],
	 * long)
	 */
	public Long hincrBy(byte[] key, byte[] field, long value) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.hincrBy(key, field, value);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#hexists(byte[], byte[])
	 */
	public Boolean hexists(byte[] key, byte[] field) {
		Boolean result = false;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.hexists(key, field);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#hdel(byte[], byte[])
	 */
	public Long hdel(byte[] key, byte[] field) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.hdel(key, field);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#hlen(byte[])
	 */
	public Long hlen(byte[] key) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.hlen(key);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Set<byte[]> hkeys(byte[] key) {
		Set<byte[]> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.hkeys(key);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Collection<byte[]> hvals(byte[] key) {
		Collection<byte[]> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.hvals(key);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Map<byte[], byte[]> hgetAll(byte[] key) {
		Map<byte[], byte[]> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.hgetAll(key);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#rpush(byte[], byte[])
	 */
	public Long rpush(byte[] key, byte[] string) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.rpush(key, string);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#lpush(byte[], byte[])
	 */
	public Long lpush(byte[] key, byte[] string) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.lpush(key, string);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#llen(byte[])
	 */
	public Long llen(byte[] key) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.llen(key);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public List<byte[]> lrange(byte[] key, int start, int end) {
		List<byte[]> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.lrange(key, start, end);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#ltrim(byte[], int, int)
	 */
	public String ltrim(byte[] key, int start, int end) {
		String result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.ltrim(key, start, end);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#lindex(byte[], int)
	 */
	public byte[] lindex(byte[] key, int index) {
		byte[] result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.lindex(key, index);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#lset(byte[], int,
	 * byte[])
	 */
	public String lset(byte[] key, int index, byte[] value) {
		String result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.lset(key, index, value);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#lrem(byte[], int,
	 * byte[])
	 */
	public Long lrem(byte[] key, int count, byte[] value) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.lrem(key, count, value);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#lpop(byte[])
	 */
	public byte[] lpop(byte[] key) {
		byte[] result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.lpop(key);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#rpop(byte[])
	 */
	public byte[] rpop(byte[] key) {
		byte[] result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.rpop(key);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#sadd(byte[], byte[])
	 */
	public Long sadd(byte[] key, byte[] member) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.sadd(key, member);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Set<byte[]> smembers(byte[] key) {
		Set<byte[]> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.smembers(key);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#srem(byte[], byte[])
	 */
	public Long srem(byte[] key, byte[] member) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.srem(key, member);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#spop(byte[])
	 */
	public byte[] spop(byte[] key) {
		byte[] result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.spop(key);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#scard(byte[])
	 */
	public Long scard(byte[] key) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.scard(key);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#sismember(byte[],
	 * byte[])
	 */
	public Boolean sismember(byte[] key, byte[] member) {
		Boolean result = false;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.sismember(key, member);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#srandmember(byte[])
	 */
	public byte[] srandmember(byte[] key) {
		byte[] result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.srandmember(key);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#zadd(byte[], double,
	 * byte[])
	 */
	public Long zadd(byte[] key, double score, byte[] member) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zadd(key, score, member);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Set<byte[]> zrange(byte[] key, int start, int end) {
		Set<byte[]> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zrange(key, start, end);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#zrem(byte[], byte[])
	 */
	public Long zrem(byte[] key, byte[] member) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zrem(key, member);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#zincrby(byte[], double,
	 * byte[])
	 */
	public Double zincrby(byte[] key, double score, byte[] member) {
		Double result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zincrby(key, score, member);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#zrank(byte[], byte[])
	 */
	public Long zrank(byte[] key, byte[] member) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zrank(key, member);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#zrevrank(byte[], byte[])
	 */
	public Long zrevrank(byte[] key, byte[] member) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zrevrank(key, member);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Set<byte[]> zrevrange(byte[] key, int start, int end) {
		Set<byte[]> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zrevrange(key, start, end);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Set<Tuple> zrangeWithScores(byte[] key, int start, int end) {
		Set<Tuple> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zrangeWithScores(key, start, end);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Set<Tuple> zrevrangeWithScores(byte[] key, int start, int end) {
		Set<Tuple> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zrevrangeWithScores(key, start, end);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#zcard(byte[])
	 */
	public Long zcard(byte[] key) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zcard(key);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#zscore(byte[], byte[])
	 */
	public Double zscore(byte[] key, byte[] member) {
		Double result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zscore(key, member);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public List<byte[]> sort(byte[] key) {
		List<byte[]> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.sort(key);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public List<byte[]> sort(byte[] key, SortingParams sortingParameters) {
		List<byte[]> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.sort(key, sortingParameters);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#zcount(byte[], double,
	 * double)
	 */
	public Long zcount(byte[] key, double min, double max) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zcount(key, min, max);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Set<byte[]> zrangeByScore(byte[] key, double min, double max) {
		Set<byte[]> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zrangeByScore(key, min, max);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Set<byte[]> zrangeByScore(byte[] key, double min, double max, int offset, int count) {
		Set<byte[]> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zrangeByScore(key, min, max, offset, count);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max) {
		Set<Tuple> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zrangeByScoreWithScores(key, min, max);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max, int offset, int count) {
		Set<Tuple> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zrangeByScoreWithScores(key, min, max, offset, count);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min) {
		Set<byte[]> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zrevrangeByScore(key, max, min);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min, int offset, int count) {
		Set<byte[]> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zrevrangeByScore(key, max, min, offset, count);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min) {
		Set<Tuple> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zrevrangeByScoreWithScores(key, max, min);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min, int offset, int count) {
		Set<Tuple> result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zrevrangeByScoreWithScores(key, max, min, offset, count);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#zremrangeByRank(byte[],
	 * int, int)
	 */
	public Long zremrangeByRank(byte[] key, int start, int end) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zremrangeByRank(key, start, end);

		} catch (Exception e) {

			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.AbsterGo.redis.serviceImpl.redisService#zremrangeByScore(byte[],
	 * double, double)
	 */
	public Long zremrangeByScore(byte[] key, double start, double end) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.zremrangeByScore(key, start, end);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Long linsert(byte[] key, LIST_POSITION where, byte[] pivot, byte[] value) {
		Long result = null;
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = shardedJedis.linsert(key, where, pivot, value);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public List<Object> pipelined(ShardedJedisPipeline shardedJedisPipeline) {
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		List<Object> result = null;
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.pipelined(shardedJedisPipeline);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Jedis getShard(byte[] key) {
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		Jedis result = null;
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.getShard(key);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Jedis getShard(String key) {
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		Jedis result = null;
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.getShard(key);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public JedisShardInfo getShardInfo(byte[] key) {
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		JedisShardInfo result = null;
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.getShardInfo(key);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public JedisShardInfo getShardInfo(String key) {
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		JedisShardInfo result = null;
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.getShardInfo(key);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.AbsterGo.redis.serviceImpl.redisService#getKeyTag(java.lang.String)
	 */
	public String getKeyTag(String key) {
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		String result = null;
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.getKeyTag(key);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Collection<JedisShardInfo> getAllShardInfo() {
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		Collection<JedisShardInfo> result = null;
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.getAllShardInfo();

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}

	public Collection<Jedis> getAllShards() {
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		Collection<Jedis> result = null;
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = shardedJedis.getAllShards();

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}



//	public Object getObjectByList(String key, Class modelType) {
//		// TODO Auto-generated method stub
//		Object result = null;
//		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
//		if (shardedJedis == null) {
//			return result;
//		}
//		boolean broken = false;
//		try {
//
//			result = SerializeUtil.deserialize(shardedJedis.get(key),List.class, modelType);
//
//		} catch (Exception e) {
//			log.error(e.getMessage(), e);
//			broken = true;
//		} finally {
//			BasicJedisService.returnResource(shardedJedis, broken);
//		}
//		return result;
//	}



	public JSON getListOfObject(String key) {
		// TODO Auto-generated method stub
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		JSON result = null;
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {

			result = SerializeUtil.deserialize(shardedJedis.get("key"),List.class);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}


	public JSON getObject(String key) {
		// TODO Auto-generated method stub
		ShardedJedis shardedJedis = BasicJedisService.getJedisCon();
		JSON result = null;
		if (shardedJedis == null) {
			return result;
		}
		boolean broken = false;
		try {
			result = SerializeUtil.deserialize(shardedJedis.get("key"),Object.class);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			broken = true;
		} finally {
			BasicJedisService.returnResource(shardedJedis, broken);
		}
		return result;
	}
}
