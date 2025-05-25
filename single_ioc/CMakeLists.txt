add_executable(single_ioc main.cpp)
target_compile_features(single_ioc PRIVATE cxx_std_20)
target_compile_options(single_ioc PRIVATE -O3 -g)
target_link_libraries(single_ioc PRIVATE pthread crypto ssl)

