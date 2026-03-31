FROM debezium/connect:2.5

USER root

COPY config /kafka/connectors
COPY start-with-connectors.sh /usr/local/bin/start-with-connectors.sh

RUN chmod +x /usr/local/bin/start-with-connectors.sh \
 && chown -R kafka:kafka /kafka/connectors /usr/local/bin/start-with-connectors.sh

USER kafka

ENTRYPOINT ["/usr/local/bin/start-with-connectors.sh"]
