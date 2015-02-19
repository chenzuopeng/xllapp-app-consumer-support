package org.xllapp.app.consumer.support;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import javax.jms.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.InitializingBean;

import org.xllapp.app.core.ScheduledTask;
import org.xllapp.jms.BatchJMSConsumer;
import org.xllapp.jms.MessageHelper;
import org.xllapp.jms.ThreadPool;

/**
 * 消息处理程序的基类.
 * 
 * @author dylan.chen Dec 30, 2013
 * 
 */
public abstract class JMSConsumerTask extends ScheduledTask implements InitializingBean {
	
	private final static Logger successMessageLogger = LoggerFactory.getLogger("jms.message.consumer.success");

	private final static Logger failedMessageLogger = LoggerFactory.getLogger("jms.message.consumer.failed");

	protected Logger logger;

	private BatchJMSConsumer batchJMSConsumer;

	public JMSConsumerTask() {
		this.logger = LoggerFactory.getLogger(this.getClass());
	}

	@Override
	public void doTask() {

		List<Message> messages = null;

		try {

			messages = this.batchJMSConsumer.getMessages();

		} catch (Exception e) {
			this.logger.error("failure to get messages", e);
		}

		if (messages != null && !messages.isEmpty()) {
			
			if (isMultithread()) {
				
				final CountDownLatch countDownLatch = new CountDownLatch(messages.size());
				
				for (final Message message : messages) {

					ThreadPool.execute(new Runnable() {
						@Override
						public void run() {
							handleMessageInternal(message);
							countDownLatch.countDown();
						}
					});

				}
				
				try {
					countDownLatch.await();
				} catch (InterruptedException e) {
					this.logger.warn("interrupted countDownLatch.await()");
				}
				
			}else{
				
				for (final Message message : messages) {
					handleMessageInternal(message);
				}
				
			}
			
		}

	}

	protected void handleMessageInternal(Message message) {

		String msgId = MessageHelper.getMessageID(message);
		
		MDC.put(Constants.LOG_MDC_MESSAGE_ID, msgId);

		try {

			handleMessage(message);

			if (this.logger.isDebugEnabled()) {
				this.logger.debug("handled message[{},{}]", msgId, MessageHelper.toString(message));
			}

			if (successMessageLogger.isInfoEnabled()) {
				successMessageLogger.info("message[{},{}]", msgId, MessageHelper.toString(message));
			}
			
		} catch (Exception e) {

			this.logger.error("handle message[" + msgId + "," + MessageHelper.toString(message) + "] error", e);

			if (failedMessageLogger.isInfoEnabled()) {
				failedMessageLogger.info("message[{},{}],caused by {}", msgId, MessageHelper.toString(message), e.getLocalizedMessage());
			}
		}finally{
			MDC.remove(Constants.LOG_MDC_MESSAGE_ID);
		}

	}

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

	public boolean isMultithread() {
		return false;
	}

	public abstract void handleMessage(Message message) throws Exception;

	public abstract String getBrokerUrl();

	public abstract String getUserName();

	public abstract String getPassword();

	public abstract String getQueue();

}
