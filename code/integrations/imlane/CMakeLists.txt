cmake_minimum_required(VERSION 3.20)
project(IMLaneFixture LANGUAGES C CXX)
set(CMAKE_CXX_STANDARD 17)
set(BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(BUILD_IMLANE_SERVER ON CACHE BOOL "" FORCE)
list(PREPEND CMAKE_PREFIX_PATH
  "${IMLANE_SOURCE}/third_party/fmt/dist" "${IMLANE_SOURCE}/third_party/boost/dist")
find_package(fmt REQUIRED)
find_package(Boost REQUIRED COMPONENTS system program_options)
find_package(Arrow REQUIRED)
get_target_property(ARROW_INCLUDE_DIRS Arrow::arrow_shared INTERFACE_INCLUDE_DIRECTORIES)
include_directories("${IMLANE_SOURCE}/third_party/json/single_include"
  "${IMLANE_SOURCE}/third_party/libuv/include" "${IMLANE_SOURCE}/third_party/fmt/dist/include"
  ${ARROW_INCLUDE_DIRS})
add_subdirectory("${IMLANE_SOURCE}" author)
add_executable(imlane_fixture_cpp fixture.cpp)
target_include_directories(imlane_fixture_cpp PRIVATE
  "${IMLANE_SOURCE}/include" "${IMLANE_SOURCE}/third_party/spdlog"
  "${IMLANE_SOURCE}/third_party/boost/dist/include")
target_link_libraries(imlane_fixture_cpp PRIVATE dbendrt_cxx fmt::fmt Boost::system)
set_target_properties(imlane_fixture_cpp PROPERTIES INSTALL_RPATH
  "${CMAKE_INSTALL_PREFIX}/lib;${PYARROW_LIBRARY_DIR};${IMLANE_SOURCE}/third_party/fmt/dist/lib;${IMLANE_SOURCE}/third_party/boost/dist/lib")
install(TARGETS imlane_fixture_cpp RUNTIME DESTINATION bin)
