# Lista tópicos que estão rodando em  um cluster local
kafka-topics.sh --bootstrap-server localhost:9092 --list

# Lista detalhadamente os tópicos do servidor
#
# --topic <nome> -> pode ser usado para descrever apenas um tópico
kafka-topics.sh --bootstrap-server localhost:9092 --describe

# Cria um novo tópico sem especificar o número de partições
#
# --if-not-exists -> pode ser usado para só criar o tópico caso ele não
# exista
#
# --partitions <n> -> pode ser usado para definir o número de partições
# do tópico sendo criado
#
# --replication-factor <n> -> pode ser usado para definir o número de
# replicas do tópico, só vai ser realmente aplicado caso haja mais de
# um broker conectado no cluster
kafka-topics.sh --bootstrap-server localhost:9092 \
    --create \           # Operação de criação de tópico
    --topic foo_topic \  # Nome do tópico sendo criado
    --partitions 3       # Quantidade de partições no tópico

# Altera um tópico aumentando o número de partições
kafka-topics.sh --bootstrap-server localhost:9092 \
    --alter \  # Operação de atualização de um tópico
    --topic foo_topic \
    --partitions 3

