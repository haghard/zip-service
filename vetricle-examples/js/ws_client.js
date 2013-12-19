var vertx = require('vertx')
var console = require('vertx/console')

var client = vertx.createHttpClient().host("192.168.0.143").port(9000);

client.connectWebsocket('/webApp', function(websocket) {
  websocket.dataHandler(function(data) {
    console.log('Received data ' + data);
    client.close();
  });
  websocket.writeTextFrame('Hello world');
});
