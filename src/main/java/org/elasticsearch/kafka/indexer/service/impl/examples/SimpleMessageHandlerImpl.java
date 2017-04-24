/**
  * @author marinapopova
  * Feb 24, 2016
 */
package org.elasticsearch.kafka.indexer.service.impl.examples;

import org.elasticsearch.kafka.indexer.exception.IndexerESNotRecoverableException;
import org.elasticsearch.kafka.indexer.exception.IndexerESRecoverableException;
import org.elasticsearch.kafka.indexer.service.ElasticSearchBatchService;
import org.elasticsearch.kafka.indexer.service.IMessageHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

/**
 * 
 * This is an example of a customized Message Handler - by implementing IMessageHandler interface
 * and using the ElasticSearchBatchService to delegate most of the non-customized logic
 *
 */
public class SimpleMessageHandlerImpl implements IMessageHandler {

	@Autowired
	private ElasticSearchBatchService elasticSearchBatchService = null;
	@Value("${esIndexName:my_index}")
	private String indexName;
	private String routingValue = null; // we don't need routing for this simple scenario


	@Override

	public String transformMessage(String inputMessage, Long offset) throws Exception {
		// do not do any transformations for this scenario - just return the message as is
		return inputMessage;
	}

	@Override
	public void addMessageToBatch(String inputMessage, String indexType, String eventUUID) throws Exception {

		elasticSearchBatchService.addEventToBulkRequest(
				inputMessage, indexName, indexType, eventUUID, routingValue);
	}

	@Override
	public void updateMessageToBatch(String inputMessage, String indexType, String eventUUID) throws Exception {
		elasticSearchBatchService.updateEventToBulkRequest(
				inputMessage, indexName, indexType, eventUUID, routingValue);
	}

	@Override
	public void deleteMessageToBatch(String indexType, String eventUUID) throws Exception {
		elasticSearchBatchService.deleteEventRequest(indexName, indexType, eventUUID, routingValue);
	}

	@Override
	public void postToElasticSearch() throws InterruptedException, IndexerESRecoverableException, IndexerESNotRecoverableException {
		elasticSearchBatchService.postToElasticSearch();
	}
}
