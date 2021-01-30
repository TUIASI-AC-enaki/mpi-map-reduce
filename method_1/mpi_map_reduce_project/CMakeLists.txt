cmake_minimum_required(VERSION 3.17)
project(mpi_map_reduce_project)

set(CMAKE_CXX_STANDARD 11)

option(USE_MPI "Enable MPI"  ON )
option(USE_OMP "Enable OpenMP"  ON )

find_package(OpenMP)
if (OPENMP_FOUND)
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")
endif()

SET(CMAKE_C_COMPILER /home/enaki/.openmpi/bin/mpicc)
SET(CMAKE_CXX_COMPILER /home/enaki/.openmpi/bin/mpicxx)

add_executable(map_reduce map_reduce.cpp utils/directory_utils.h utils/file_reader.h utils/word_parser.h utils/printer.h algorithm/mapper.h algorithm/master.h algorithm/reducer.h algorithm/shared.h)

add_custom_command(TARGET map_reduce PRE_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_directory
        ${CMAKE_SOURCE_DIR}/input/ $<TARGET_FILE_DIR:map_reduce>/input/)