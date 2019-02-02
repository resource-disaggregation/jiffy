find_package(Maven REQUIRED)

set(JIFFY_CLIENT_ARTIFACTS ${CMAKE_CURRENT_SOURCE_DIR}/target/jiffy4j-${PROJECT_VERSION}.jar
        ${CMAKE_CURRENT_SOURCE_DIR}/target/jiffy4j-${PROJECT_VERSION}.pom)
set(STORAGE_EXEC "${CMAKE_CURRENT_BINARY_DIR}/../storage/storaged")
set(DIRECTORY_EXEC "${CMAKE_CURRENT_BINARY_DIR}/../directory/directoryd")

set(JAVA_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src/main/java)
set(JAVA_TEST_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src/test/java)
file(GLOB JAVA_SOURCES ${JAVA_SRC_DIR}/*.java)
file(GLOB JAVA_TESTS ${JAVA_TEST_DIR}/*.java)

add_custom_command(OUTPUT ${JIFFY_CLIENT_ARTIFACTS}
        DEPENDS ${JAVA_SOURCES} ${JAVA_TESTS}
        COMMAND ${Maven_EXECUTABLE} clean package -DskipTests=true
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_custom_target(javaclient ALL
        COMMENT "Building Jiffy Java Client library using Maven"
        DEPENDS ${JIFFY_CLIENT_ARTIFACTS})

if (BUILD_TESTS)
  add_test(NAME JavaClientTest
          COMMAND ${Maven_EXECUTABLE} -Djiffy.directory.exec=${DIRECTORY_EXEC} -Djiffy.storage.exec=${STORAGE_EXEC} test
          WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
endif ()