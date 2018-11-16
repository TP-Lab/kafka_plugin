if(BUILD_KAFKA_PLUGIN)
  include(FindPkgConfig)
  pkg_search_module(RDKAFKA REQUIRED rdkafka)

  add_library( kafka_plugin
               kafka_plugin.cpp kafka_producer.cpp
               ${HEADERS} )
  target_include_directories(kafka_plugin
	  PUBLIC "include" ${RDKAFKA_INCLUDEDIR}/librdkafka
          )
  target_link_libraries(kafka_plugin
	  PUBLIC chain_plugin eosio_chain appbase fc ${RDKAFKA_LIBRARIES}
          )
else()
  message("kafka_plugin not selected and will be omitted.")
endif()
