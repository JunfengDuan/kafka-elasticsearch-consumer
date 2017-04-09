package org.elasticsearch.kafka.indexer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created by dhyan on 1/28/16.
 */
public class KafkaESIndexerProcess {
    private static final Logger logger = LoggerFactory.getLogger(KafkaESIndexerProcess.class);
    public static void main(String[] args) throws Exception {

        logger.info("Starting KafkaESIndexerProcess  ");

        ClassPathXmlApplicationContext indexerContext = new ClassPathXmlApplicationContext("spring/kafka-es-context-public.xml");
        indexerContext.registerShutdownHook();

        logger.info("KafkaESIndexerProcess is started OK");

    }
}
