package Com.AbsterGo.redis.api;

import java.util.List;

import com.alibaba.fastjson.JSON;

public class SerializeUtil {

	  public static  String serialize(Object target) {
	        return JSON.toJSONString(target);
	    }

	   public static JSON deserialize(String jsonString, Class clazz) {
	        // 序列化结果应该是List对象
	        if (clazz.isAssignableFrom(List.class)) {
	            return JSON.parseArray(jsonString);
	        }
	        // 序列化结果是普通对象
	        return JSON.parseObject(jsonString);
	    }
}
