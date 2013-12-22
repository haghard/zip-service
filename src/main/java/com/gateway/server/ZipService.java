package com.gateway.server;

import org.vertx.java.core.*;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpServerFileUpload;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.core.http.ServerWebSocket;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Verticle;
import java.util.Map;
import java.util.regex.Pattern;

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
  private String ALL_PRODUCTS;
  private String PRODUCT_BY_ID;

  private Logger logger;

  private JsonObject config;

  private final RouteMatcher router = new RouteMatcher();

  private static final Pattern pattern = Pattern.compile( "[\\d]+" );

  private static final String RESULT_FIELD = "result";

  private static final String BUS_NAME = "gateway/system";
  private static final String MONGO_MODULE_NAME = "mongo-persistor";
  private static final String JDBS_MODULE_NAME = "com.bloidonia.jdbcpersistor";

  private static final String VERTIX_MONGO_MODULE_NAME = "io.vertx~mod-mongo-persistor~2.1.0";
  private static final String VERTIX_JDBC_MODULE_NAME = "com.bloidonia~mod-jdbc-persistor~2.1";

  private static final String INDEX_PAGE = "form/index.html";
  private static final String UPLOAD_PAGE = "form/upload.html";
  private static final String UPLOAD_TO_DIR = "uploads/";

  private final String servicePath = new java.io.File(".").getAbsolutePath().replace(".","");

  private void init()
  {
    logger = container.logger();
    config = container.config();
    logger.info( "Config :" + config.toString() );

    port = config.getObject( "network-settings" ).getInteger( "port" );

    final JsonObject sqlQueries = config.getObject( "sql-query" );
    ALL_PRODUCTS = sqlQueries.getString( "products" );
    PRODUCT_BY_ID = sqlQueries.getString( "productById" );

    initPersistors( config );
    initEventBus();
  }

  public void start()
  {
    init();

    router.get( "/", new Handler<HttpServerRequest>()
    {
      @Override
      public void handle( final HttpServerRequest req )
      {
        req.response().sendFile( servicePath + INDEX_PAGE );
      }
    } );

    router.get( "/uploads", new Handler<HttpServerRequest>()
    {
      @Override
      public void handle( final HttpServerRequest req )
      {
        req.response().sendFile( servicePath + UPLOAD_PAGE );
      }
    } );

    router.post( "/uploads", new Handler<HttpServerRequest>()
    {
      @Override
      public void handle( final HttpServerRequest req )
      {
        req.expectMultiPart( true );
        req.uploadHandler( new Handler<HttpServerFileUpload>()
        {
          @Override
          public void handle( final HttpServerFileUpload upload )
          {
            upload.exceptionHandler( new Handler<Throwable>()
            {
              @Override
              public void handle( Throwable event )
              {
                req.response().end( "Upload failed" );
              }
            } );
            upload.endHandler( new Handler<Void>()
            {
              @Override
              public void handle( Void event )
              {
                req.response().end( "Upload successful, you should see the file in the server directory" );
              }
            } );
            upload.streamToFileSystem( servicePath + UPLOAD_TO_DIR + upload.filename() );
          }
        } );
      }
    } );

    router.post( "/form", new Handler<HttpServerRequest>()
    {
      @Override
      public void handle( final HttpServerRequest req )
      {
        logger.info( "post data from form" );
        req.response().setChunked( true );
        req.expectMultiPart( true );
        req.endHandler( new VoidHandler()
        {
          protected void handle()
          {
            for ( Map.Entry<String, String> entry : req.formAttributes() )
            {
              req.response().write( "Got attr " + entry.getKey() + " : " + entry.getValue() + "\n" );
            }
            req.response().end();
          }
        } );
      }
    } );

    //http://192.168.0.143:9000/products
    router.get( "/products", new Handler<HttpServerRequest>()
    {
      @Override
      public void handle( final HttpServerRequest httpRequest )
      {
        final JsonObject query = new JsonObject()
                .putString( "action", "select" )
                .putString( "stmt", ALL_PRODUCTS );

        logger.info( query );
        vertx.eventBus().send( JDBS_MODULE_NAME, query, new Handler<Message<JsonObject>>()
        {
          @Override
          public void handle( Message<JsonObject> message )
          {
            if ( message.body().getString( "status" ).equalsIgnoreCase( "ok" ) )
            {
              final JsonArray result = message.body().getArray( RESULT_FIELD );
              httpRequest.response().putHeader( "Content-Type", "application/json" );
              httpRequest.response().end( result.encodePrettily() );
            } else
            {
              httpRequest.response().end( message.body().toString() );
            }
          }
        } );
      }
    } );

    //http://192.168.0.143:9000/product/244
    router.get( "/product/:id", new Handler<HttpServerRequest>()
    {
      @Override
      public void handle( final HttpServerRequest httpRequest )
      {
        final MultiMap params = httpRequest.params();
        if ( params.contains( "id" ) )
        {
          final String productId = params.get( "id" );
          if ( pattern.matcher( productId ).matches() )
          {
            final JsonArray values = new JsonArray();
            values.add( productId );

            final JsonObject query = new JsonObject()
                    .putString( "action", "select" ).putString( "stmt", PRODUCT_BY_ID )
                    .putArray( "values", values );

            logger.info( query );
            vertx.eventBus().send( JDBS_MODULE_NAME, query, new Handler<Message<JsonObject>>()
            {
              @Override
              public void handle( Message<JsonObject> message )
              {
                logger.info( message.body() );
                if ( message.body().getString( "status" ).equalsIgnoreCase( "ok" ) )
                {
                  final JsonArray result = message.body().getArray( RESULT_FIELD );
                  httpRequest.response().putHeader( "Content-Type", "application/json" );
                  httpRequest.response().end( result.encodePrettily() );
                } else
                {
                  httpRequest.response().end( message.body().toString() );
                }
              }
            } );
          } else
          {
            httpRequest.response().end( " id should be a digit: " + productId );
          }
        }
      }
    } );

    router.get( "/zips", new Handler<HttpServerRequest>()
    {
      public void handle( final HttpServerRequest req )
      {

        JsonObject json = new JsonObject();
        final MultiMap params = req.params();

        if ( params.size() > 0 && params.contains( "state" ) || params.contains( "city" ) )
        {
          JsonObject matcher = new JsonObject();
          if ( params.contains( "state" ) )
          {
            matcher.putString( "state", params.get( "state" ) );
            logger.info( "find state by " + params.get( "state" ) );
          }

          if ( params.contains( "city" ) )
          {
            matcher.putString( "city", params.get( "city" ) );
            logger.info( "find by city " + params.get( "city" ) );
          }

          json = new JsonObject().putString( "collection", "zips" )
                  .putString( "action", "find" )
                  .putObject( "router", matcher );

        } else
        {
          json = new JsonObject().putString( "collection", "zips" )
                  .putString( "action", "find" )
                  .putObject( "router", new JsonObject() );
        }

        JsonObject data = new JsonObject();
        data.putArray( "results", new JsonArray() );
        vertx.eventBus().send( MONGO_MODULE_NAME, json, new ReplyHandler( req, data ) );
      }
    } );

    router.get( "/zip/:id", new Handler<HttpServerRequest>()
    {
      public void handle( final HttpServerRequest req )
      {
        final String zipId = req.params().get( "id" );

        final JsonObject matcher = new JsonObject().putString( "_id", zipId );
        final JsonObject json = new JsonObject().putString( "collection", "zips" )
                .putString( "action", "find" )
                .putObject( "router", matcher );

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
            } else
            {
              req.response().end( zipId + " not exists" );
            }
          }
        } );
      }
    } );

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
          } );
        }
        else
        {
          webSocket.reject();
        }
      }
    } )
            .requestHandler( router )
            .listen( port );
  }

  private void initPersistors( JsonObject config )
  {
    container.deployModule( VERTIX_MONGO_MODULE_NAME, config.getObject( MONGO_MODULE_NAME ), 1, new AsyncResultHandler<String>()
    {
      @Override
      public void handle( AsyncResult<String> stringAsyncResult )
      {
        logger.info( VERTIX_MONGO_MODULE_NAME + " say " + stringAsyncResult.result() );
      }
    } );

    container.deployModule( VERTIX_JDBC_MODULE_NAME, config.getObject( JDBS_MODULE_NAME ), 1, new AsyncResultHandler<String>()
    {
      @Override
      public void handle( AsyncResult<String> stringAsyncResult )
      {
        logger.info( VERTIX_JDBC_MODULE_NAME + " say " + stringAsyncResult.result() );
      }
    } );
  }

  private void initEventBus()
  {
    vertx.eventBus().registerHandler( BUS_NAME, new Handler<Message<String>>()
    {
      @Override
      public void handle( Message<String> message )
      {
        container.logger().info( BUS_NAME + "from " + message.replyAddress() + " Body: " + message.body() );
        message.reply( "pong!" );
      }
    } );
  }
}