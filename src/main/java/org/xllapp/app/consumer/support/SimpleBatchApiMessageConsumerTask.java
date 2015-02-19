package org.xllapp.app.consumer.support;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.jms.Message;

import org.apache.activemq.command.ActiveMQMapMessage;
import org.springframework.beans.factory.InitializingBean;
import org.xllapp.app.consumer.support.ApiMessageConsumerTask.ApiMessage;

import org.xllapp.app.core.ScheduledTask;
import org.xllapp.jms.BatchJMSConsumer;
import org.xllapp.jms.MessageHelper;
import org.xllapp.mybatis.BatchTemplate;

/**
 * 
 * 批量将来自com.ffcs.icity.api.core.JMSController子类的消息插入到数据库中.
 * 
 * @author dylan.chen Nov 16, 2014
 * 
 */
public abstract class SimpleBatchApiMessageConsumerTask extends ScheduledTask implements InitializingBean {

	private BatchJMSConsumer batchJMSConsumer;

	@Override
	public void doTask() {

		List<Message> messages = null;
		try {
			messages = this.batchJMSConsumer.getMessages();
		} catch (Exception e) {
			this.logger.error("failure to get messages", e);
		}

		if (messages == null || messages.isEmpty()) {
			return;
		}

		this.logger.info("extract message count:{}", messages.size());

		List<Object> list = new ArrayList<Object>();
		for (final Message message : messages) {
			try {
				ApiMessage apiMessage = resolveApiMessage(message);
				list.addAll(resolveSqlParams(apiMessage));
			} catch (Exception e) {
				this.logger.error("failure to resolve message[" + MessageHelper.toString(message) + "],caused by " + e.getLocalizedMessage(), e);
			}
		}

		BatchTemplate.execute(list, new BatchTemplate.BatchCallback<Object>() {
			@Override
			public void doBatch(int i, List<Object> batch) {
				executeSql(batch);
			}
		});

	}

	public abstract void executeSql(List<Object> batch);

	protected ApiMessage resolveApiMessage(Message message) throws Exception {
		if (message instanceof ActiveMQMapMessage) {
			ActiveMQMapMessage mapMessage = (ActiveMQMapMessage) message;
			Map<String, Object> msgContent = mapMessage.getContentMap();
			return new ApiMessage(msgContent);
		} else {
			throw new Exception("unsupported message");
		}
	}

	public abstract List<Object> resolveSqlParams(ApiMessage message) throws Exception;

	@Override
	public void afterPropertiesSet() throws Exception {
		this.batchJMSConsumer = new BatchJMSConsumer(getBrokerUrl(), getUserName(), getPassword(), getQueue(), getBatchSize());
	}

	public int getBatchSize() {
		return 10;
	}

	@Override
	public long getDelay() {
		return 10000;
	}

	public abstract String getBrokerUrl();

	public abstract String getUserName();

	public abstract String getPassword();

	public abstract String getQueue();

}
