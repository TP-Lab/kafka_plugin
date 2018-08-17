#kafka_plugin

## install librdkafka
```
#git clone https://github.com/edenhill/librdkafka.git
#cd librdkafka
#./configure
#make
#sudo make install
```
## 增加kafka plugin
```
#cd /usr/local/eos/plugins/
#git clone https://github.com/tokenbankteam/kafka_plugin.git
#edit /usr/local/eos/plugins/CMakeLists.txt:
#add_subdirectory(kafka_plugin)
#edit /usr/local/eos/programs/nodeos/CMakeLists.txt:
#target_link_libraries( nodeos PRIVATE -Wl,${whole_archive_flag} kafka_plugin -Wl,${no_whole_archive_flag} )
#parmeters for kafka_plugin
# --plugin eosio::kafka_plugin
# --kafka-uri 192.168.31.225:9092
# --accept_trx_topic eos_accept_topic
# --applied_trx_topic eos_applied_topic
# --kafka-block-start 100
# --kafka-queue-size 5000
```
