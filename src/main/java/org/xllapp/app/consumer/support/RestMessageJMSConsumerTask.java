package org.xllapp.app.consumer.support;

import java.util.Map;

import javax.jms.Message;

import org.apache.activemq.command.ActiveMQMapMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 处理来自icity-api-jms模块消息的Consumer的基类.
 * 
 * @author dylan.chen Nov 2, 2013
 * 
 */
public abstract class RestMessageJMSConsumerTask extends JMSConsumerTask {

	@Override
	public void handleMessage(Message message) throws Exception {
		if (message instanceof ActiveMQMapMessage) {
			ActiveMQMapMessage mapMessage = (ActiveMQMapMessage) message;
			Map<String, Object> msgContent = mapMessage.getContentMap();
			RestMessage restMessage = new RestMessage(msgContent);
			handleRestMessage(restMessage);
		} else {
			throw new Exception("unsupported message");
		}
	}

	public abstract void handleRestMessage(RestMessage message) throws Exception;

	public static class RestMessage {
		
		private final static Logger logger = LoggerFactory.getLogger(RestMessage.class);
		
		private static final String REQUEST_PARAM_KEY = "request_param";

		private static final String BODY_PARAM_NAME= "request.body";

		private final static String HEAD_PREFIX="head.";
		
		private Map<String,Object> params;
		
		@SuppressWarnings("unchecked")
		public RestMessage(Map<String,Object> message){
			this.params=(Map<String, Object>)message.get(REQUEST_PARAM_KEY);
			logger.debug("request: {}",params);
		}
		
		/**
		 * 获取请求头.
		 * 
		 * @param name 请求头名
		 * @return 请求头的值
		 */
		public String getHeader(String name){
			return (String)params.get(HEAD_PREFIX+name);
		}
		
		/**
		 * 获取POST请求中的表单参数或查询参数.
		 * 
		 * @param name 参数名
		 * @return 参数的值
		 */
		public String getParam(String name){
			return (String)params.get(name);
		}
		
		/**
		 * 获取使用POST方式传递的请求体,如放在请求体中的JSON内容等.
		 * 
		 * @return 请求体
		 */
		public String getBody(){
			return (String)params.get(BODY_PARAM_NAME);
		}

	}
	
}
