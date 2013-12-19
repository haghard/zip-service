package com.gateway.server.test.integration.java;

import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.WebSocket;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

import java.io.*;

import static org.vertx.testtools.VertxAssert.*;
import static org.vertx.testtools.VertxAssert.assertEquals;

/**
 * Example Java integration test that deploys the module that this project builds.
 * <p/>
 * Quite often in integration tests you want to deploy the same module for all tests and you don't want tests
 * to start before the module has been deployed.
 * <p/>
 * This test demonstrates how to do that.
 */
public class ModuleIntegrationTest extends TestVerticle
{
  int port;
  @Test
  public void testEventBus()
  {
    vertx.eventBus().send("gateway/system", "ping!", new Handler<Message<String>>() {
      @Override
      public void handle(Message<String> reply) {
        assertEquals("pong!", reply.body());
        container.logger().info( "pong receiver" );
        testComplete();
      }
    });
  }

  @Test
  public void testWebSocket()
  {
    vertx.createHttpClient().setHost( "localhost" ).setPort( port )
      .connectWebsocket( "/webApp", new Handler<WebSocket>()
      {
        @Override
        public void handle( WebSocket webSocket )
        {
          webSocket.dataHandler( new Handler<Buffer>()
          {
            @Override
            public void handle( Buffer buffer )
            {
              assertTrue( buffer.toString().equals( "Hello from webSocket server" ) );
              testComplete();
            }
          } );
          //
          webSocket.writeTextFrame( "Req websocket" );
        }
      } );
  }

  /*@Test
  public void testMultipleGet()
  {
    int size = 5;
    final HttpClient[] clients = new HttpClient[size];

    for ( int i = 0; i < size; i++ )
    {
      clients[i] = vertx.createHttpClient().setPort( 9000 );
    }

    for ( int i = 35004; i < 35104; i++ )
    {
      final HttpClient client = clients[i % size];
      client.getNow( "/zips/" + i, new Handler<HttpClientResponse>()
      {
        @Override
        public void handle( HttpClientResponse event )
        {
          event.bodyHandler( new Handler<Buffer>()
          {
            @Override
            public void handle( Buffer event )
            {
              final String body = event.getString( 0, event.length() );
              assertNotNull( body );
              testComplete();
            }
          });
        }
      });
    }
  }*/

  @Override
  public void start()
  {
    // Make sure we call initialize() - this sets up the assert stuff so assert functionality works correctly
    initialize();
    // Deploy the module - the System property `vertx.modulename` will contain the name of the module so you
    // don't have to hardecode it in your tests

    System.out.println( System.getProperty( "vertx.modulename" ) );

    final StringBuffer strBuf = new StringBuffer();
    final BufferedReader in;
    try
    {
      in = new BufferedReader( new FileReader( new File( "config.json" ) ) );
      String line;
      while ((line = in.readLine()) != null)
      {
        strBuf.append(line);
      }

      final JsonObject cfg = new JsonObject(strBuf.toString());
      final JsonObject settings = cfg.getObject( "network-settings" );
      port = settings.getInteger( "port" );

      container.deployModule( System.getProperty( "vertx.modulename" ), cfg, new AsyncResultHandler<String>()
      {
        @Override
        public void handle( AsyncResult<String> asyncResult )
        {
          // Deployment is asynchronous and this this handler will be called when it's complete (or failed)
          if ( asyncResult.failed() )
          {
            container.logger().error( asyncResult.cause() );
          }
          assertTrue( asyncResult.succeeded() );
          assertNotNull( "deploymentID should not be null", asyncResult.result() );
          // If deployed correctly then start the tests!
          startTests();
        }
      });

    }
    catch ( IOException e )
    {
      e.printStackTrace();
    }
  }
}
