#pragma once


#include <eosio/rabbitmq_plugin/rabbitmq_producer.hpp>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fc/io/json.hpp>
#include <fc/utf8.hpp>
#include <fc/variant.hpp>


#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <amqp_framing.h>
#include <eosio/chain/controller.hpp>
#include <eosio/chain/trace.hpp>
#include <eosio/chain_plugin/chain_plugin.hpp>

namespace eosio {




                              
                                                            
void exception_on_amqp_error(amqp_rpc_reply_t x, char const *context) {
  switch (x.reply_type) {
    case AMQP_RESPONSE_NORMAL:
      return;

    case AMQP_RESPONSE_NONE:
      fprintf(stderr, "%s: missing RPC reply type!\n", context);
      FC_THROW_EXCEPTION(rabbitmq_plugin_no_reply_exception, "no reply" );
      break;

    case AMQP_RESPONSE_LIBRARY_EXCEPTION:
      fprintf(stderr, "%s: %s\n", context, amqp_error_string2(x.library_error));
      FC_THROW_EXCEPTION(rabbitmq_plugin_library_exception, "rabbitmq error: ${c}: ${e}\n", 
        ("c", context)("e", amqp_error_string2(x.library_error))
        );
      break;

    case AMQP_RESPONSE_SERVER_EXCEPTION:
      switch (x.reply.id) {
        case AMQP_CONNECTION_CLOSE_METHOD: {
          amqp_connection_close_t *m =
              (amqp_connection_close_t *)x.reply.decoded;
          fprintf(stderr, "%s: server connection error %uh, message: %.*s\n",
                  context, m->reply_code, (int)m->reply_text.len,
                  (char *)m->reply_text.bytes);
          // throw closed exception
          FC_THROW_EXCEPTION(rabbitmq_plugin_closed_connection_exception, "closed connection" );
          break;
        }
        case AMQP_CHANNEL_CLOSE_METHOD: {
          amqp_channel_close_t *m = (amqp_channel_close_t *)x.reply.decoded;
          fprintf(stderr, "%s: server channel error %uh, message: %.*s\n",
                  context, m->reply_code, (int)m->reply_text.len,
                  (char *)m->reply_text.bytes);
          FC_THROW_EXCEPTION(rabbitmq_plugin_closed_channel_exception, "closed channel" );
          break;
        }
        default:
          fprintf(stderr, "%s: unknown server error, method id 0x%08X\n",
                  context, x.reply.id);
          FC_THROW_EXCEPTION(rabbitmq_plugin_unknown_exception, "%{c}: unknown server error",
                  ("c", context) );
          break;
      }
      break;
  }

}


int rabbitmq_producer::trx_rabbitmq_init(std::string hostname, uint32_t port, std::string username,std::string password){

  conn = amqp_new_connection();
  m_hostname = hostname;
  m_port = port;
  m_username = username;
  m_password = password;
  
  socket = amqp_tcp_socket_new(conn);
  EOS_ASSERT( socket, rabbitmq_plugin_open_connection_exception, "failed creating rabbitmq socket");
  
  auto status = amqp_socket_open(socket, hostname.c_str(), port);
  EOS_ASSERT( !status, rabbitmq_plugin_open_connection_exception, "failed opening connection socket");
  

  exception_on_amqp_error(amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN,
                               username.c_str(), password.c_str()),"login");
  amqp_channel_open(conn, 1);
  exception_on_amqp_error(amqp_get_rpc_reply(conn), "Opening channel");
  return 0;
}

void rabbitmq_producer::trx_rabbitmq_sendmsg(std::string routingKey, std::string exchange, std::string msgstr){
  amqp_basic_properties_t props;
  props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
  props.content_type = amqp_cstring_bytes("text/plain");
  props.delivery_mode = 2; /* persistent delivery mode */
  

  auto status = amqp_basic_publish(conn, 1, amqp_cstring_bytes(exchange.c_str()),
                                  amqp_cstring_bytes(routingKey.c_str()), 0, 0,
                                  &props, amqp_cstring_bytes(msgstr.c_str()));

  // dlog("sending message ${e}", ("e", exchange));
  EOS_ASSERT( !status, rabbitmq_plugin_publish_exception, "failed publish");             
  // dlog("message sent ${m}", ("m", msgstr));

}

void rabbitmq_producer::trx_rabbitmq_destroy(){
  exception_on_amqp_error(amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS),
                    "Closing channel");
  exception_on_amqp_error(amqp_connection_close(conn, AMQP_REPLY_SUCCESS),
                    "Closing connection");
  amqp_destroy_connection(conn);
}


}

