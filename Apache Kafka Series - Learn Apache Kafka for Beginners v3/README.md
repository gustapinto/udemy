# Instalação

## Zookeeper + Kafka

1. Criar pastas dos volumes:
   ```bash
   mkdir kafka_data
   mkdir zookeeper_data
   ```
2. Dar permissão para os volumes:
   ```bash
   sudo chown -R 1001:1001 kafka_data zookeeper_data
   sudo chown -R 1000:1000 opensearch_data
   ```
3. Executar docker-compose
   ```bash
   docker-compose -f zookeeper-stack.yml up --build
   ```
