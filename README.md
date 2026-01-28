# Bet365 Live Scraper ⚽🏀

> **DISCLAIMER:** Este projeto é apenas para fins de estudo e pesquisa sobre WebSockets e interceptação de tráfego. O uso em produção pode violar os termos de serviço da Bet365.

Um sistema robusto para capturar dados em tempo real da Bet365, interceptando o tráfego WebSocket diretamente do navegador e disponibilizando-o através de uma API local fácil de consumir via JSON.

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Python](https://img.shields.io/badge/python-3.8%2B-blue)
![Chrome](https://img.shields.io/badge/chrome-extension-google)

## 📚 Documentação

- [**Guia de Arquitetura**](ARCHITECTURE.md): Explicação técnica detalhada de como o sistema funciona, diagramas de fluxo de dados e estrutura interna.

## 🚀 Funcionalidades

- **Tempo Real**: Baixa latência (milisegundos) entre o evento no jogo e a API.
- **WebSocket Hook**: Interceptação direta (`hook.js`) sem necessidade de emular tokens ou cookies complexos.
- **API Simples**: Consuma dados sanitizados via JSON (`GET /live`).
- **Multiesporte**: Suporte nativo para Futebol e Basquete.

## 🛠️ Instalação e Uso

### 1. API Local (Backend)

Instale as dependências e rode o servidor:

```bash
pip install flask flask-cors pytz
python local_api.py
```
*O servidor iniciará em `http://127.0.0.1:8485`*

### 2. Extensão do Chrome

1. Abra o Chrome e vá para `chrome://extensions/`
2. Ative o **Modo do desenvolvedor** (canto superior direito).
3. Clique em **Carregar sem compactação** ("Load unpacked").
4. Selecione a pasta `chrome_extention` deste repositório.

### 3. Rodando o Scraper

1. Com o `local_api.py` rodando, abra o site da **Bet365** no navegador com a extensão instalada.
2. Navegue até a seção "Ao Vivo" (In-Play).
3. A extensão começará a enviar dados automaticamente para o seu servidor local.
4. Verifique os dados recebidos:
   - **Endpoint de Jogos**: [http://127.0.0.1:8485/live?sport=1](http://127.0.0.1:8485/live?sport=1) (Futebol)
   - **Debug**: O console do servidor Python mostrará logs de `insert` e atualizações.

## ⚙️ Configuração

Se precisar alterar a porta ou URL da API:
1. Clique no ícone da extensão no Chrome.
2. Insira a nova URL (ex: `http://localhost:9090/data`).
3. Clique em Save.

---
*Desenvolvido com foco em performance e simplicidade.*
