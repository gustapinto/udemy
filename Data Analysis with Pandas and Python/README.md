# Data analysis with Pandas and Python

## Instalação do ambiente

1. Inicie o ambiente docker com o comando:
   ```bash
   docker-compose up -d --build
   ```
2. Entre no terminal do container `anaconda`:
   ```bash
   docker-compose exec anaconda bash
   ```
3. Crie e ative um novo ambiente `conda`:
   ```bash
   conda create --name pandas_playground && conda activate pandas_playground
   ```
4. Instale as bibliotecas necessárias:
   ```bash
   # Usando o processo manual
   conda install pandas jupyter bottleneck numexpr matplotlib

   # Ou instalando a partir do arquivo de requirements
   conda install --file requirements.txt
   ```

## Iniciando os notebooks Jupyter

```bash
jupyter notebook --allow-root --ip 0.0.0.0
```
