# Produzindo uma nova mensagem no tópico foo_topic, esse comando irá
# abrir uma interação para que digitemos a mensagem
#
# --producer-property -> pode ser usado para especificar propriedades
# adicionais no produtor
kafka-console-producer.sh --bootstrap-server localhost:9092 \
    --topic foo_topic # Define o tópico em que o produtor publicará as mensagens

# Produzindo mensagens com chaves separadas seguindo o padrão <chave>:<valor>
kafka-console-producer.sh --bootstrap-server localhost:9092 \
    --topic foo_topic \
    --property parse.key=true \  # Define se deve ou não parsear as chaves
    --property key.separator=:   # Define o padrão para identificar as chaves e os valores

# Produzindo mensagens e replicando-as pelas partições usando um partitioner
#
# OBS: não usar em produção, pois a performance é muito afetada
kafka-console-producer.sh --bootstrap-server localhost:9092 \
    --producer-property partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner \
    --topic foo_topic_3

