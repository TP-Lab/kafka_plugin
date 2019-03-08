# EOSIO Kafka Plugin
## what's eosio kafka plugin
EOSIO Kafka Plugin is used to receive the transaction data fom blockchain and send out the transaction through kafka producer. Developer can receive the transaction data through kafka consumer in the background application.

## how does the kafka plugin work
#1.it run a task to resume the transactions on chain. there's two type of transactions:"applied transaction" and "accepted transaction"
#2.create two kafka topics, the producer of which store the applied transaction and accepted transaction in kafka queue
#3.the dapp developer can get the transaction data through the consumer of the kafka topic.

## Based eosio version
#EOS-Mainnet/eos mainnet-1.6.1 or later

## Building the plugin [Install on your nodeos server]
#1. install kafka library
```
#cd /usr/local
#git clone https://github.com/edenhill/librdkafka.git
#cd librdkafka
#./configure
#make
#sudo make install
```
#2.download the kafka plugin code in to eos file
```
#cd /usr/local/eos/plugins/
#git clone https://github.com/tokenbankteam/kafka_plugin.git
```
#3.update the CMakeLists.txt to complie the kafka plugin 
```
edit /usr/local/eos/plugins/CMakeLists.txt:
#add_subdirectory(kafka_plugin)

edit /usr/local/eos/programs/nodeos/CMakeLists.txt:
#target_link_libraries( nodeos PRIVATE -Wl,${whole_archive_flag} kafka_plugin -Wl,${no_whole_archive_flag} )
```

## How to setup on your nodeos
Enable this plugin using --plugin option to nodeos or in your config.ini. Use nodeos --help to see options used by this plugin.

## Configuration
Add the following to config.ini to enable the plugin:
```
parmeters for kafka_plugin
# --plugin eosio::kafka_plugin
# --kafka-uri 192.168.31.225:9092
# --accept_trx_topic eos_accept_topic
# --applied_trx_topic eos_applied_topic
# --kafka-block-start 100
# --kafka-queue-size 5000
```
