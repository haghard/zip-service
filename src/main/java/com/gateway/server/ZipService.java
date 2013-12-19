package com.gateway.server;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.core.http.ServerWebSocket;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Verticle;


/*
import rx.Observable;
import rx.util.functions.Action1;
import rx.util.functions.Func1;
import io.vertx.rxcore.RxSupport;
import io.vertx.rxcore.java.eventbus.RxEventBus;
import io.vertx.rxcore.java.eventbus.RxMessage;
*/


//https://github.com/vert-x/mod-mongo-persistor
//http://192.168.0.143:9000/zips?city=ACMAR&state=AL
//http://192.168.0.143:9000/zips?city=ACMAR
//http://192.168.0.143:9000/zip/35004

//1. Http service
//2. WebSocket service
//3. EventBus
public class ZipService extends Verticle
{
  private int port;
  private Logger logger;

  private static final String BUS_NAME = "gateway/system";
  private static final String MONGO_MODULE_NAME = "mongo-persistor";
  private static final String VERTIX_MONGO_MODULE_NAME = "io.vertx~mod-mongo-persistor~2.1.0";

  //private final RxEventBus rxEventBus = new RxEventBus(vertx.eventBus());

  public void start()
  {
    logger = container.logger();

    final JsonObject config = container.config();
    logger.info( "Config: " + config.toString() );

    final JsonObject settings = config.getObject( "network-settings" );

    port = settings.getInteger( "port" );
    logger.info( "Read config: port " + port );

    initMongoPersistor( config );
    initEventBus();

    final RouteMatcher matcher = new RouteMatcher();

    matcher.get("/zips", new Handler<HttpServerRequest>() {
      public void handle(final HttpServerRequest req) {

        JsonObject json = new JsonObject();
        final MultiMap params = req.params();

        if (params.size() > 0 && params.contains("state") || params.contains("city")) {
          JsonObject matcher = new JsonObject();
          if (params.contains("state")) {
            matcher.putString("state", params.get("state"));
            logger.info( "find state by " + params.get( "state" ) );
          }

          if (params.contains("city")) {
            matcher.putString("city", params.get("city"));
            logger.info( "find by city " + params.get( "city" ) );
          }

          json = new JsonObject().putString("collection", "zips")
                  .putString("action", "find")
                  .putObject("matcher", matcher);

        } else {
          json = new JsonObject().putString("collection", "zips")
                  .putString("action", "find")
                  .putObject("matcher", new JsonObject());
        }

        JsonObject data = new JsonObject();
        data.putArray("results", new JsonArray());
        vertx.eventBus().send(MONGO_MODULE_NAME, json, new ReplyHandler(req, data));
      }
    });

    matcher.get( "/zip/:id", new Handler<HttpServerRequest>()
    {
      public void handle( final HttpServerRequest req )
      {
        final String zipId = req.params().get( "id" );

        final JsonObject matcher = new JsonObject().putString( "_id", zipId );
        final JsonObject json = new JsonObject().putString( "collection", "zips" )
                .putString( "action", "find" )
                .putObject( "matcher", matcher );

        logger.info( "Try to find " + zipId );
        vertx.eventBus().send( MONGO_MODULE_NAME, json, new Handler<Message<JsonObject>>()
        {
          @Override
          public void handle( Message<JsonObject> event )
          {
            if ( event.body().getArray( "results" ).size() > 0 )
            {
              JsonObject result = event.body().getArray( "results" ).get( 0 );
              req.response().putHeader( "Content-Type", "application/json" );
              req.response().end( result.encodePrettily() );
            }
            else
            {
              req.response().end( zipId + " not exists" );
            }
          }
        });
      }
    });

    /*matcher.post("/rxzip/:id", new Handler<HttpServerRequest>() {
      public void handle(final HttpServerRequest req) {
        // first access the buffer as an observable. We do this this way, since
        // we want to keep using the matchhandler and we can't do that with rxHttpServer
        Observable<Buffer> reqDataObservable = RxSupport.toObservable( req );

        // after we have the body, we update the element in the database
        Observable<RxMessage<JsonObject>> updateObservable = reqDataObservable.flatMap(new Func1<Buffer, Observable<RxMessage<JsonObject>>>() {
          @Override
          public Observable<RxMessage<JsonObject>> call(Buffer buffer) {
            System.out.println("buffer = " + buffer);
            // create the message
            JsonObject newObject = new JsonObject(buffer.getString(0, buffer.length()));
            JsonObject matcher = new JsonObject().putString("_id", req.params().get("id"));
            JsonObject json = new JsonObject().putString("collection", "zips")
                    .putString("action", "update")
                    .putObject("criteria", matcher)
                    .putBoolean("upsert", false)
                    .putBoolean("multi", false)
                    .putObject("objNew", newObject);

            // and return an observable
            return rxEventBus.send("mongodb-persistor", json);
          }
        });

        // use the previous input again, so we could see whether the update was successful.
        Observable<RxMessage<JsonObject>> getLatestObservable = updateObservable.flatMap(new Func1<RxMessage<JsonObject>, Observable<RxMessage<JsonObject>>>() {
          @Override
          public Observable<RxMessage<JsonObject>> call(RxMessage<JsonObject> jsonObjectRxMessage) {
            System.out.println("jsonObjectRxMessage = " + jsonObjectRxMessage);
            // next we get the latest version from the database, after the update has succeeded
            // this isn't dependent on the previous one. It just has to wait till the previous
            // one has updated the database, but we could check whether the previous one was successfully
            JsonObject matcher = new JsonObject().putString("_id", req.params().get("id"));
            JsonObject json2 = new JsonObject().putString("collection", "zips")
                    .putString("action", "find")
                    .putObject("matcher", matcher);
            return rxEventBus.send(MONGO_MODULE_NAME, json2);
          }
        });

        // after we've got the latest version we return this in the response.
        getLatestObservable.subscribe(new Action1<RxMessage<JsonObject>>() {
          @Override
          public void call(RxMessage<JsonObject> jsonObjectRxMessage) {
            req.response().end(jsonObjectRxMessage.body().encodePrettily());
          }
        });
      }
    });
    */

    matcher.get( "/api", new Handler<HttpServerRequest>()
    {
      @Override
      public void handle( HttpServerRequest req )
      {
        req.response().end( "<body> <tab>/zips/:id</tab> <tab> params: city, street</tab>  </body>" );
      }
    });

    vertx.createHttpServer().websocketHandler( new Handler<ServerWebSocket>()
    {
      @Override
      public void handle( final ServerWebSocket webSocket )
      {
        if ( webSocket.path().contains( "/webApp" ) )
        {
          webSocket.dataHandler( new Handler<Buffer>()
          {
            @Override
            public void handle( Buffer buf )
            {
              webSocket.writeTextFrame( "Hello from webSocket server" );
            }
          });
        }
        else
        {
          webSocket.reject();
        }
      }
    })
    .requestHandler( matcher )
    .listen( port );
  }

  private void initMongoPersistor( JsonObject config )
  {
    container.deployModule( VERTIX_MONGO_MODULE_NAME, config.getObject( MONGO_MODULE_NAME ) );
  }

  private void initEventBus()
  {
    vertx.eventBus().registerHandler( BUS_NAME, new Handler<Message<String>>()
    {
      @Override
      public void handle( Message<String> message )
      {
        container.logger().info( "Gateway/system from " + message.replyAddress() + " Body: " + message.body() );
        message.reply( "pong!" );
      }
    } );
  }
}