package com.gateway.server;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

public class ReplyHandler implements Handler<Message<JsonObject>>
{

  private final HttpServerRequest request;
  private JsonObject data;

  ReplyHandler( final HttpServerRequest request, JsonObject data )
  {
    this.request = request;
    this.data = data;
  }

  @Override
  public void handle( Message<JsonObject> event )
  {
    // if the response contains more message, we need to get the rest
    if ( event.body().getString( "status" ).equals( "more-exist" ) )
    {
      JsonArray results = event.body().getArray( "results" );

      for ( Object el : results )
      {
        data.getArray( "results" ).add( el );
      }

      event.reply( new JsonObject(), new ReplyHandler( request, data ) );
    }
    else
    {

      JsonArray results = event.body().getArray( "results" );
      for ( Object el : results )
      {
        data.getArray( "results" ).add( el );
      }

      request.response().putHeader( "Content-Type", "application/json" );
      request.response().end( data.encodePrettily() );
    }
  }
}
