package org.elasticsearch.kafka.indexer.jobs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.elasticsearch.kafka.indexer.FailedEventsLogger;
import org.elasticsearch.kafka.indexer.exception.IndexerESNotRecoverableException;
import org.elasticsearch.kafka.indexer.exception.IndexerESRecoverableException;
import org.elasticsearch.kafka.indexer.service.IMessageHandler;
import org.elasticsearch.kafka.indexer.service.OffsetLoggingCallbackImpl;
import org.elasticsearch.kafka.indexer.service.impl.examples.SimpleMessageHandlerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author marinapopova Apr 13, 2016
 */
public class ConsumerWorker implements Runnable {

	private static final Logger logger = LoggerFactory.getLogger(ConsumerWorker.class);
	private static final String PARENTID = "parentId";
	private static final String PARENTName = "parentTableName";
	private IMessageHandler messageHandler;
	private KafkaConsumer<String, String> consumer;
	private final List<String> kafkaTopics;
	private final int consumerId;
	// interval in MS to poll Kafka brokers for messages, in case there were no
	// messages during the previous interval
	private long pollIntervalMs;
	private OffsetLoggingCallbackImpl offsetLoggingCallback;
	private AtomicBoolean topicIsChanged = new AtomicBoolean(false);

	private static ConsumerManager consumerManager = new ConsumerManager();


	public ConsumerWorker(int consumerId, String consumerInstanceName, List<String> kafkaTopics, Properties kafkaProperties,
			long pollIntervalMs, IMessageHandler messageHandler) {
		this.messageHandler = messageHandler;
		kafkaProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerInstanceName + "-" + consumerId);
		this.consumer = new KafkaConsumer<>(kafkaProperties);

		this.consumerId = consumerId;
		this.kafkaTopics = kafkaTopics;
		this.pollIntervalMs = pollIntervalMs;
		offsetLoggingCallback = new OffsetLoggingCallbackImpl();
		logger.info(
				"Created ConsumerWorker with properties: consumerId={}, consumerInstanceName={}, kafkaTopic={}, kafkaProperties={}",
				consumerId, consumerInstanceName, kafkaTopics);
	}

	private void addOrUpdateMessageToBatch(String processedMessage, String topic, String id) throws Exception{

		com.alibaba.fastjson.JSONObject json = com.alibaba.fastjson.JSON.parseObject(processedMessage);
		String parentId = json.containsKey(PARENTID)? (String)json.get(PARENTID) : "";
		String parentTableName = json.containsKey(PARENTName)? (String)json.get(PARENTName) : "";
		if(StringUtils.isBlank(parentId)){
			messageHandler.addMessageToBatch(processedMessage, topic, id);
		}else{
			JSONObject josnMessage = new JSONObject();
			josnMessage.put(topic, getJsonArray(processedMessage));
			JSONObject inputMessage = new JSONObject(josnMessage);
			logger.info("\ninputMessage:{}\nparentTableName:{}", inputMessage, parentTableName);
			messageHandler.upDateMessageToBatch(inputMessage.toString(), parentTableName, parentId);
		}
	}

	private JSONArray getJsonArray(String message){
		Object obj = JSON.parse(message);
		JSONArray array = new JSONArray();

		if(obj instanceof JSONObject){
			array.add(obj);
			return array;
		}else{
			return (JSONArray) obj;
		}
	}

	@Override
	public void run() {
		try {
			logger.info("Starting ConsumerWorker, consumerId={}", consumerId);
			consumer.subscribe(kafkaTopics, offsetLoggingCallback);
			while (true) {
				boolean isPollFirstRecord = true;
				int numProcessedMessages = 0;
				int numSkippedIndexingMessages = 0;
				int numMessagesInBatch = 0;
				long offsetOfNextBatch = 0;

				logger.debug("consumerId={}; about to call consumer.poll() ...", consumerId);
				ConsumerRecords<String, String> records = consumer.poll(pollIntervalMs);
				Map<Integer, Long> partitionOffsetMap = new HashMap<>();

				// processing messages and adding them to ES batch
				for (ConsumerRecord<String, String> record : records) {
					numMessagesInBatch++;
					Map<String, Object> data = new HashMap<>();
					data.put("partition", record.partition());
					data.put("offset", record.offset());
					data.put("value", record.value());

					logger.debug("consumerId={}; recieved record: {}", consumerId, data);
					if (isPollFirstRecord) {
						isPollFirstRecord = false;
						logger.info("Start offset for partition {} in this poll : {}", record.partition(),
								record.offset());
					}

					try {
						String processedMessage = messageHandler.transformMessage(record.value(), record.offset());
						addOrUpdateMessageToBatch(processedMessage, record.topic(), record.key());
						partitionOffsetMap.put(record.partition(), record.offset());
						numProcessedMessages++;
					} catch (Exception e) {
						numSkippedIndexingMessages++;

						logger.error("ERROR processing message {} - skipping it: {}", record.offset(), record.value(),
								e);
						FailedEventsLogger.logFailedToTransformEvent(record.offset(), e.getMessage(), record.value());
					}

				}

				logger.info(
						"Total # of messages in this batch: {}; "
								+ "# of successfully transformed and added to Index: {}; # of skipped from indexing: {}; offsetOfNextBatch: {}",
						numMessagesInBatch, numProcessedMessages, numSkippedIndexingMessages, offsetOfNextBatch);

				// push to ES whole batch
				boolean moveToNextBatch = false;
				if (!records.isEmpty()) {				
					moveToNextBatch = postToElasticSearch();
				}
				
				if (moveToNextBatch) {
					logger.info("Invoking commit for partition/offset : {}", partitionOffsetMap);
					consumer.commitAsync(offsetLoggingCallback);
				}

				List<String> newTopics = topicIsChanged();
				if(newTopics.size()>0){
					logger.info("Kafka topics are changed, new topics are :{}",kafkaTopics);
					consumer.subscribe(kafkaTopics, offsetLoggingCallback);
					consumer.commitSync(getPartitionAndOffset(newTopics.get(0),0));
				}

			}

		} catch (WakeupException e) {
			logger.warn("ConsumerWorker [consumerId={}] got WakeupException - exiting ... Exception: {}", consumerId,
					e.getMessage());
			// ignore for shutdown
		} 
		
		catch (IndexerESNotRecoverableException e){
			logger.error("ConsumerWorker [consumerId={}] got IndexerESNotRecoverableException - exiting ... Exception: {}", consumerId,
					e.getMessage());
		}
		catch (Exception e) {
			// TODO handle all kinds of Kafka-related exceptions here - to stop
			// / re-init the consumer when needed
			logger.error("ConsumerWorker [consumerId={}] got Exception - exiting ... Exception: {}", consumerId,
					e.getMessage());
		} finally {
			if (consumer != null){
				logger.warn("ConsumerWorker [consumerId={}] is shutting down ...", consumerId);
				consumer.close();
			}
		}
	}

	private List<String> topicIsChanged(){
		ConsumerManager consumerManager = new ConsumerManager();
		List<String> topics = consumerManager.getKafkaTopics(consumer);
		List<String> remain = new ArrayList<>();
		if(topics.size() > kafkaTopics.size()){
			remain.addAll(topics);
			remain.removeAll(kafkaTopics);
			kafkaTopics.addAll(topics);
		}
		return remain;
	}

	private Map getPartitionAndOffset(String topic, long offset){
		Map map = new HashMap();
		List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
		PartitionInfo partitionInfo = partitionInfos.get(0);
		map.put(new TopicPartition(partitionInfo.topic(),partitionInfo.partition()),new OffsetAndMetadata(offset));
		return map;
	}

	private boolean postToElasticSearch() throws InterruptedException, IndexerESNotRecoverableException{
		boolean moveToTheNextBatch = true;
		try {
			messageHandler.postToElasticSearch();
		} catch (IndexerESRecoverableException e) {
			moveToTheNextBatch = false;
			logger.error("Error posting messages to Elastic Search - will re-try processing the batch; error: {}",
            e.getMessage());
		} 
		
		return moveToTheNextBatch;
	}

	public void shutdown() {
		logger.warn("ConsumerWorker [consumerId={}] shutdown() is called  - will call consumer.wakeup()", consumerId);
		consumer.wakeup();
	}

	public Map<TopicPartition, OffsetAndMetadata> getPartitionOffsetMap() {
		return offsetLoggingCallback.getPartitionOffsetMap();
	}

	public int getConsumerId() {
		return consumerId;
	}
}
