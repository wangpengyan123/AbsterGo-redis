package test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Random;

import org.AbsterGo.redis.service.BasicJedisService;
import org.AbsterGo.redis.service.redisService;
import org.AbsterGo.redis.service.Impl.redisServiceImpl;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import Com.AbsterGo.redis.api.SerializeUtil;
import redis.clients.jedis.ShardedJedisPool;

public class test {
	public static void main(String[] args) {
//   ClassPathXmlApplicationContext cp=new ClassPathXmlApplicationContext("classpath:spring-mvc.xml");
//            cp.start();
//            BasicJedisService jedis=new  BasicJedisService();
//            jedis.setShardedJedisPool((ShardedJedisPool) cp.getBean("shardedJedisPool"));
//            redisService redis=new redisServiceImpl();
//            redis.set("name","123");

//            redis.setObject(user.getUserId(),user);
//            for(int i=10000;i<20000;i++){
//                redis.lpush("list","wang"+i+"am");	 	
//            }
//
//            redis.ltrim("list", 0,5000);
//            System.out.println("success");
//            List list=redis.hvals("user");
//            System.out.println();
            
            
//            AppUser user=new AppUser();
//            user.setUserId("010206");
//            user.setAge("17");
//            user.setName("liu"); 
//            user.setPwd("ww8877");
//            List<Map<String ,Object>> list=new ArrayList<Map<String,Object>>();
//            Map map=new HashMap();
//            map.put("user",user);
//            map.put("wang","wang");
//            list.add(map);
//           redis.zadd("msg".getBytes(),Double.parseDouble(user.getUserId()),SerializeUtil.serialize(list));
//             redis.hset(user.getUserId(),"name" ,user.getName());
//             redis.hset(user.getUserId(),"age" ,user.getAge();
//             Map map=   redis.hgetAll(user.getUserId());
	}
}




