/**
 * Created with IntelliJ IDEA.
 * User: haghard
 * Date: 17/12/13
 * Time: 19:48
 * To change this template use File | Settings | File Templates.
 */

var eb = require("vertx/event_bus")
var vertx = require("vertx/console")

vertx.requestHandler(function(req) {
    console.log("Received: " + req)
    //req.response.sendFile('./web/index.html'); // Always serve the index page
}).listen(8080, 'foo.com')
