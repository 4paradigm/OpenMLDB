FetchContent_Declare(
  gflags
  GIT_REPOSITORY https://github.com/gflags/gflags
  GIT_TAG e171aa2d15ed9eb17054558e0b3a6a413bb01067 # v2.2.2
)
set(BUILD_gflags_LIB ON)
set(GFLAGS_NAMESPACE google)
FetchContent_MakeAvailable(gflags)
