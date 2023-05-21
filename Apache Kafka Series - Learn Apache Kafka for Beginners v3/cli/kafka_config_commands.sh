# Listando as configurações disponíveis
kafka-configs.sh

# Alterando as configurações de um tópico
kafka-configs.sh --bootstrap-server localhost:9092 \
    --entity-type topics \    # Define o tipo de entidade que vai ser configurada
    --entity-name foo_topic \ # Define a entidade configurada
    --alter \
    --add-config min.insync.replicas=2 # Adiciona uma configuração

