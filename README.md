# EOSIO RabbitMQ Plugin (WIP)
EOSIO RabbitMQ Plugin

## Requirements
###  install librabbitmq
```
yum install librabbitmq-devel.x86_64
```
or
```
apt-get install librabbitmq-dev
```
or
```
git clone https://github.com/alanxz/rabbitmq-c.git
cd rabbitmq-c/cmake
cmake -DCMAKE_INSTALL_PREFIX=/usr ..
sudo cmake --build . --config Release --target install
```

## Building the plugin [Install on your nodeos server]
```
#cd /usr/local/eos/plugins/
#git clone https://github.com/tmuskal/eos-rabbitmq-plugin.git rabbitmq_plugin

edit /usr/local/eos/plugins/CMakeLists.txt:
#add_subdirectory(rabbitmq_plugin)

edit /usr/local/eos/programs/nodeos/CMakeLists.txt:
#target_link_libraries( nodeos PRIVATE -Wl,${whole_archive_flag} rabbitmq_plugin -Wl,${no_whole_archive_flag} )
```
## How to setup on your nodeos
Enable this plugin using --plugin option to nodeos or in your config.ini. Use nodeos --help to see options used by this plugin.

## Setup queues
```
rabbitmqadmin declare exchange name=trx.accepted type=direct durable=true internal=false 'arguments={"alternate-exchange":"trx.accepted_alt.fanout"}'

rabbitmqadmin declare exchange name=trx.applied type=direct durable=true internal=false 'arguments={"alternate-exchange":"trx.accepted_alt.fanout"}'
```
## Configuration
Add the following to config.ini to enable the plugin:
```
parmeters for rabbitmq_plugin
# --plugin eosio::rabbitmq_plugin
# --rabbitmq-uri 192.168.31.225:9092
# --accept_trx_topic eos_accept_topic
# --applied_trx_topic eos_applied_topic
# --rabbitmq-block-start 100
# --rabbitmq-queue-size 5000
```
