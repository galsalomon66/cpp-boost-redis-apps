add_executable(multiple_ioc main.cpp)
target_compile_features(multiple_ioc PRIVATE cxx_std_20)
target_compile_options(multiple_ioc PRIVATE -O3 -g)
target_link_libraries(multiple_ioc PRIVATE pthread crypto ssl)

