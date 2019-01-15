  file(GLOB HEADERS "include/enumivo/mongo_db_plugin/*.hpp")
  include_directories("/usr/local/include/librdkafka")
  LINK_LIBRARIES("/usr/local/lib/librdkafka.so" "/usr/local/lib/librdkafka.so.1")
  link_directories("/usr/local/lib")
  add_library( kafka_plugin
               kafka_plugin.cpp kafka_producer.cpp
               ${HEADERS} )
target_include_directories(kafka_plugin
          PUBLIC "include"
          )
  target_link_libraries(kafka_plugin
          PUBLIC chain_plugin enumivo_chain appbase fc
          )

