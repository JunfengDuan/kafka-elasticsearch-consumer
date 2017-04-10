package org.elasticsearch.kafka.indexer.service;

import org.elasticsearch.kafka.indexer.exception.IndexerESNotRecoverableException;
import org.elasticsearch.kafka.indexer.exception.IndexerESRecoverableException;

/**
 * Created by dhyan on 1/28/16.
 */
public interface IMessageHandler {

    /**
     * Add messages to Batch
     * @param inputMessage
     * @param offset
     * @throws Exception
     */
    public void addMessageToBatch(String inputMessage, String indexType, String eventUUID) throws Exception;

    public String transformMessage(String inputMessage, Long offset) throws Exception;
    

    /**
     * In most cases - do not customize this method, just delegate to the BasicMessageHandler implementation
     * @return
     * @throws Exception
     */
    public void postToElasticSearch() throws InterruptedException, IndexerESRecoverableException, IndexerESNotRecoverableException;
    


}
