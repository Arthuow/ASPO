# ASPO - Aplicação de Processamento de Dados

Este projeto processa arquivos ZIP contendo dados de medição elétrica e os importa para um banco de dados SQLite.

## Configuração para Desenvolvimento

### Pré-requisitos
- Python 3.8+
- VS Code (recomendado)

### Instalação

1. **Ativar o ambiente virtual**
   ```powershell
   .\.venv\Scripts\Activate.ps1
   ```

2. **Instalar dependências**
   ```powershell
   pip install -r requirements.txt
   ```

### Execução

#### Modo Desenvolvimento (Recomendado)
```powershell
python SQLITE_dev.py
```

#### Modo Produção
```powershell
python SQLITE.py
```

### Funcionalidades do Modo Desenvolvimento

- **Logging detalhado**: Logs mais verbosos salvos em `aspo_dev.log`
- **Backup automático**: Cria backup do banco antes de operações críticas
- **Validação de dados**: Verifica integridade dos DataFrames
- **Profiling**: Mede tempo de execução das funções
- **Tratamento de erros melhorado**: Traceback completo em caso de erros

### Estrutura do Projeto

```
ASPO_App/
├── SQLITE.py              # Versão original/produção
├── SQLITE_dev.py          # Versão de desenvolvimento
├── dev_config.py          # Configurações de desenvolvimento
├── requirements.txt       # Dependências do projeto
├── Medicoes.db           # Banco de dados SQLite (gerado automaticamente)
├── backups/              # Backups automáticos (modo dev)
├── logs/                 # Logs detalhados (modo dev)
└── .vscode/              # Configurações do VS Code
    ├── launch.json       # Configurações de debug
    └── tasks.json        # Tarefas do VS Code
```

### Usando o VS Code

1. **Executar**: Pressione `Ctrl+Shift+P` e digite "Tasks: Run Task"
2. **Debug**: Pressione `F5` para iniciar o debug
3. **Terminal**: Use o terminal integrado do VS Code

### Tarefas Disponíveis (VS Code)

- **Executar ASPO (Desenvolvimento)**: Executa a versão de desenvolvimento
- **Executar ASPO (Produção)**: Executa a versão de produção
- **Instalar Dependências**: Instala as dependências do requirements.txt
- **Verificar Banco SQLite**: Mostra as tabelas existentes no banco

### Formato dos Arquivos de Entrada

- **Formato**: Arquivos ZIP contendo arquivos TXT
- **Estrutura do TXT**: Dados separados por ponto e vírgula (;)
- **Primeira linha**: Cabeçalho com nomes das colunas
- **Coluna obrigatória**: 'DATA/Hora' no formato datetime

### Exemplo de Uso

1. Execute o programa
2. Selecione a pasta contendo os arquivos ZIP
3. O programa processará automaticamente:
   - Extrair arquivos TXT dos ZIPs
   - Limpar e validar os dados
   - Agrupar por hora
   - Salvar no banco SQLite

### Troubleshooting

#### Erro: "no such table"
- Verifique se a tabela está sendo criada corretamente
- Execute a versão de desenvolvimento para logs detalhados

#### Erro: "memory"
- Processe arquivos menores por vez
- Aumente o limite de memória em `dev_config.py`

#### Erro: "encoding"
- Verifique se os arquivos TXT estão em encoding latin1
- Ajuste o encoding em `dev_config.py` se necessário

### Logs

#### Modo Desenvolvimento
- Console: Logs detalhados com timestamp e função
- Arquivo: `aspo_dev.log` com logs persistentes

#### Modo Produção
- Console: Logs básicos com informações principais

### Backup

No modo desenvolvimento, backups automáticos são criados em:
- `backups/Medicoes_backup_YYYYMMDD_HHMMSS.db`

### Performance

- O modo desenvolvimento inclui profiling automático
- Monitora uso de memória
- Logs de tempo de execução das funções principais
