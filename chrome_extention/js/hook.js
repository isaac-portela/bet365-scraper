// js/hook.js

function wrap(obj, meth) {
  var orig = obj[meth];

  obj[meth] = function wrapper() {
    // arguments[0] contém os dados da mensagem WebSocket
    window.dispatchEvent(new CustomEvent("sendToAPI", { detail: arguments[0] }));
    return orig.apply(this, arguments);
  };
}

function hookSocket() {
  if (window.readit && window.readit.WebsocketTransportMethod && window.readit.WebsocketTransportMethod.prototype) {
    wrap(window.readit.WebsocketTransportMethod.prototype, "socketDataCallback");
    console.log("Bet365 WebSocket hooked successfully!");
  } else {
    setTimeout(hookSocket, 1000);
  }
}

setTimeout(hookSocket, 1000);
