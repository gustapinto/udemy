# Iniciando o consumo de mensagens futuras de um tópico ("a partir de agora")
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic foo_topic

# Consumindo todas as mensagens de um tópico, desde o começo
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic foo_topic --from-beginning

# Formatando a output das mensagens consumidas usando um formatter
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic foo_topic \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property  print.timestamp=true \  # Exibe o timestamp de envio de cada mensagem
    --property print.key=true \         # Exibe a chave de cada mensagem
    --property print.value=true \       # Exibe o valor de cada mensagem
    --property print.partition=true     # Exibe a partição de cada mensagem

# Listando os grupos de consumidores
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

# Inicia um consumidor em um grupo
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic foo_topic_3 \
    --group foo \  # Nome do grupo em que esse consumidor estará atrelado
    --from-beginning

# Descrevendo detalhadamente um grupo de consumidores
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --describe \
    --group foo

# Resetando os offsets de um grupo de consumidores
#
# OBS: para executar o reset troque "--dry-run" por "--execute"
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --group foo \
    --reset-offsets \  # Flag que reseta os offsets
    --to-earliest \    # Reseta para os menores possíveis (normalmente 0)
    --topic foo_topic_3 \
    --dry-run  # Exibe a ação que vai ser executada sem executá-la de fato

