package com.gateway.server;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
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
  private static final String JDBS_MODULE_NAME = "com.bloidonia.jdbcpersistor";

  private static final String VERTIX_MONGO_MODULE_NAME = "io.vertx~mod-mongo-persistor~2.1.0";
  private static final String VERTIX_JDBC_MODULE_NAME = "com.bloidonia~mod-jdbc-persistor~2.1";

  public void start()
  {
    logger = container.logger();

    final JsonObject config = container.config();
    logger.info( "Config !!!!!!!!!!!!!: " + config.toString() );

    final JsonObject settings = config.getObject( "network-settings" );

    port = settings.getInteger( "port" );
    logger.info( "Read config: port " + port );

    initPersistors( config );
    initEventBus();

    final RouteMatcher matcher = new RouteMatcher();

    matcher.get("/products", new Handler<HttpServerRequest>() {
      public void handle( final HttpServerRequest httpRequest ) {

        final JsonObject query = new JsonObject()
          .putString( "action", "select" )
          .putString( "stmt", "SELECT * FROM product1" );

        logger.info( query );

        vertx.eventBus().send( JDBS_MODULE_NAME, query, new Handler<Message<JsonObject>>()
        {
          @Override
          public void handle( Message<JsonObject> event )
          {
            logger.info( "Status " + event.body().getString( "status" ) );
            if (event.body().getString( "status" ).equalsIgnoreCase( "ok" ))
            {
              if ( event.body().getArray( "result" ).size() > 0 )
              {
                final JsonObject result = event.body().getArray( "result" ).get( 0 );
                httpRequest.response().putHeader( "Content-Type", "application/json" );
                httpRequest.response().end( result.encodePrettily() );
              } else {
                httpRequest.response().end( " empty response " );
              }
            } else {
              httpRequest.response().end( event.body().toString() );
            }
          }
        });
      }
    });

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

  private void initPersistors( JsonObject config )
  {
    container.deployModule( VERTIX_MONGO_MODULE_NAME, config.getObject( MONGO_MODULE_NAME ), 1 , new AsyncResultHandler<String>()
    {
      @Override
      public void handle( AsyncResult<String> stringAsyncResult )
      {
        logger.info( VERTIX_JDBC_MODULE_NAME + " say " + stringAsyncResult.result() );
      }
    });

    container.deployModule( VERTIX_JDBC_MODULE_NAME, config.getObject( JDBS_MODULE_NAME ), 1, new AsyncResultHandler<String>()
    {
      @Override
      public void handle( AsyncResult<String> stringAsyncResult )
      {
        logger.info( VERTIX_MONGO_MODULE_NAME + " say " + stringAsyncResult.result() );
      }
    });
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
    });
  }
}