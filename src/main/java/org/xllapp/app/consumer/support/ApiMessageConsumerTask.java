package org.xllapp.app.consumer.support;

import java.util.Collections;
import java.util.Map;

import javax.jms.Message;

import org.apache.activemq.command.ActiveMQMapMessage;
import org.xllapp.app.consumer.support.JMSConsumerTask;

/**
 * 处理来自com.ffcs.icity.api.core.JMSController子类消息的Consumer基类.
 *
 * @author dylan.chen Nov 16, 2014
 * 
 */
public abstract class ApiMessageConsumerTask extends JMSConsumerTask {

	@Override
	public void handleMessage(Message message) throws Exception {
		if (message instanceof ActiveMQMapMessage) {
			ActiveMQMapMessage mapMessage = (ActiveMQMapMessage) message;
			Map<String, Object> msgContent = mapMessage.getContentMap();
			ApiMessage apiMessage = new ApiMessage(msgContent);
			handleRestMessage(apiMessage);
		} else {
			throw new Exception("unsupported message");
		}
	}

	public abstract void handleRestMessage(ApiMessage message) throws Exception;

	public static class ApiMessage {
		
		private static final String KEY_REQUEST_HEADERS="request.headers";
		
		private static final String KEY_REQUEST_ARGUMENTS="request.arguments";

		private Map<String, String> headers;
		
		private Map<String, Object> arguments;

		@SuppressWarnings("unchecked")
		public ApiMessage(Map<String, Object> message) {
			this.headers = (Map<String, String>)message.get(KEY_REQUEST_HEADERS);
			this.arguments = (Map<String, Object>)message.get(KEY_REQUEST_ARGUMENTS);
			
			if(null == this.headers){
				this.headers = Collections.EMPTY_MAP;
			}
			
			if(null == this.arguments){
				this.arguments = Collections.EMPTY_MAP;
			}
		}

		/**
		 * 获取请求头.
		 * 
		 * @param name
		 *            请求头名
		 * @return 请求头的值
		 */
		public String getHeader(String name) {
			return (String) this.headers.get(name);
		}

		/**
		 * 获取请求参数.
		 * 
		 * @param name
		 *            参数名
		 * @return 参数的值
		 */
		public Object getArgument(String name) {
			return this.arguments.get(name);
		}
		
		public String getIp(){
			return this.headers.get("X-Real-IP");
		}

		public Map<String, String> getHeaders() {
			return headers;
		}

		public Map<String, Object> getArguments() {
			return arguments;
		}

	}

}
