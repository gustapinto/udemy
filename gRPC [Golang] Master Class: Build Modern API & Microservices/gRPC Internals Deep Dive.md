# gRPC Internals Depp Dive

## Protocol Buffers

- Protocol Buffers são um formato binário de troca de dados entre serviços, análogos a formatos como JSON ou Avro
- gRPC usa Protocol Buffers como formato de troca de dados, eles são usados por conta de sereem mais eficientes do que outros formatos, como JSON, para trocar dados, sendo mais leves e rápidos de transferir e parsear
- Podem ser utilizados para gerar código automaticamente com base em seu schema

## HTTP/2

- Protocolo de comunicação mais rápido do que o HTTP/1.1, sendo seu sucessor
- HTTP/2 abre apenas uma conexão TCP entre o servidor e o cliente, permitindo que multiplas requisições sejam feitas com apenas essa conexão
- Suporta multiplexação de requisições
- Envia os dados e os cabeçalhos de requisição criptografados binariamente, sendo assim mais eficiente do que a geração anterior, que os enviava sob a forma de texto puro

## Tipos de APIs gRPC

- Existem quatro tipos de API gRPC:
  - Unary 
  - Server Streaming 
  - Client Streaming 
  - Bi directional streaming 

### Unary 

- Mais parecida com uma API REST
- Cliente envia uma requisição e obtém uma resposta para essa requisição a partir do servidor
- Representação em um Protocol Buffer:
  ```
  service GreetService {
      rpc Greet(GreetRequest) returns (GreetResponse) {};
  }
  ```

### Server Streaming 

- Clientes enviam uma requisição e o servidor retorna uma ou mais respostas
- Representação em um Protocol Buffer:
  ```
  service GreetService {
      rpc Greet(GreetRequest) returns (stream GreetResponse) {};
  }
  ```

### Client Streaming

- Clientes enviam uma ou mais requisições e o servidor retorna apenas uma resposta
- Representação em um Protocol Buffer:
  ```
  service GreetService {
      rpc Greet(stream GreetRequest) returns (GreetResponse) {};
  }
  ```

### Bi Directional Streaming

- Clientes enviam uma ou mais requisições e o servidor também retorna uma ou mais requisições
- Representação em um Protocol Buffer:
  ```
  service GreetService {
      rpc Greet(stream GreetRequest) returns (stream GreetResponse) {};
  }
  ```

