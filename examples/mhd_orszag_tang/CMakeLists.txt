cmake_minimum_required(VERSION 3.18)
project(orszag_tang_mhd_adios LANGUAGES CXX)

option(OT_ENABLE_MPI "Enable MPI domain decomposition" ON)

find_package(ADIOS2 REQUIRED)

if(OT_ENABLE_MPI)
  find_package(MPI REQUIRED COMPONENTS CXX)
endif()

set(OT_SOURCES
  src/main.cpp
  src/mhd2d.cpp
  src/adios_writer.cpp
)

add_executable(ot_mhd ${OT_SOURCES})
target_include_directories(ot_mhd PRIVATE src)
target_compile_features(ot_mhd PRIVATE cxx_std_17)
target_compile_options(ot_mhd PRIVATE -Wall -Wextra -Wpedantic)

if(OT_ENABLE_MPI)
  set(OT_ADIOS_CANDIDATES
    adios2::cxx_mpi
    adios2::cxx11_mpi
    adios2::cxx
    adios2::cxx11
    adios2::adios2
  )
else()
  set(OT_ADIOS_CANDIDATES
    adios2::cxx
    adios2::cxx11
    adios2::adios2
    adios2::cxx_mpi
    adios2::cxx11_mpi
  )
endif()

set(OT_ADIOS_TARGET "")
foreach(candidate IN LISTS OT_ADIOS_CANDIDATES)
  if(TARGET ${candidate})
    set(OT_ADIOS_TARGET ${candidate})
    break()
  endif()
endforeach()

if(OT_ADIOS_TARGET STREQUAL "")
  message(FATAL_ERROR "No usable ADIOS2 C++ target exported by this installation")
endif()

target_link_libraries(ot_mhd PRIVATE ${OT_ADIOS_TARGET})

if(OT_ENABLE_MPI)
  target_compile_definitions(ot_mhd PRIVATE OT_USE_MPI=1)
  target_link_libraries(ot_mhd PRIVATE MPI::MPI_CXX)
endif()
