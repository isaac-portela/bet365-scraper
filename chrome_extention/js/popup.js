/**
 * ╔══════════════════════════════════════════════════════════════════════════════╗
 * ║                  BET365 LIVE SCRAPER - POPUP SCRIPT                          ║
 * ╚══════════════════════════════════════════════════════════════════════════════╝
 * 
 * DESCRIÇÃO:
 *   Script do popup da extensão Chrome.
 *   Responsável pela interface de configuração da URL da API.
 * 
 * FUNCIONALIDADES:
 *   1. Carrega a URL atual da API do storage ao abrir o popup
 *   2. Permite ao usuário editar e salvar uma nova URL
 *   3. Envia a nova URL para o background script persistir
 * 
 * INTERFACE:
 *   - Campo de texto: Exibe/edita a URL da API
 *   - Botão Save: Salva a nova URL
 * 
 * COMUNICAÇÃO:
 *   Popup → chrome.runtime.sendMessage → Background Script → chrome.storage.local
 */

// =============================================================================
// INICIALIZAÇÃO DO POPUP
// =============================================================================

/**
 * Listener executado quando o DOM do popup está pronto.
 * 
 * Responsável por:
 * 1. Carregar a URL atual da API do chrome.storage
 * 2. Preencher o campo de texto com a URL carregada
 * 3. Configurar o handler do botão de salvar
 */
document.addEventListener("DOMContentLoaded", () => {
  const apiUrlInput = document.getElementById("apiUrl");

  // -------------------------------------------------------------------------
  // CARREGA URL ATUAL DO STORAGE
  // -------------------------------------------------------------------------
  /**
   * Obtém a URL da API salva no chrome.storage.local.
   * Se existir, preenche o campo de texto.
   */
  chrome.storage.local.get(["apiUrl"], (result) => {
    if (result.apiUrl) {
      apiUrlInput.value = result.apiUrl;
    }
  });

  // -------------------------------------------------------------------------
  // HANDLER DO BOTÃO SAVE
  // -------------------------------------------------------------------------
  /**
   * Handler do clique no botão Save.
   * 
   * Valida a URL inserida e envia para o background script.
   * O background script persiste a URL no storage e atualiza seu cache.
   * 
   * Exibe alertas de sucesso ou erro para o usuário.
   */
  document.getElementById('saveBtn').addEventListener('click', function() {
    const newApiUrl = apiUrlInput.value.trim();
    
    // Validação: URL não pode estar vazia
    if (!newApiUrl) {
      alert("Please enter a valid API URL.");
      return;
    }
    
    // Envia mensagem para o background script atualizar a URL
    chrome.runtime.sendMessage(
          { type: "SET_API_URL", apiUrl: newApiUrl },
          (response) => {
            if (response && response.success) {
              alert("API URL updated successfully.");
            } else {
              alert("Failed to update API URL.");
            }
          }
        );
    });

});
