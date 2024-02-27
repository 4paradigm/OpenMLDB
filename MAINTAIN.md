MAINTAIN
======

C dependencies
----

### Third-party dependencies

Managed in `third-party` directory as standalone cmake project, install to specific directory before top CMakeLists.txt able to find dependencies. We'll skipped pre-compiled third-party binary for specific OSs only: centos 7, ubuntu 20.04, macos >= 12. So the three OSs listed above archive a faster build time, otherwise, a build for third-party dependencies is necessary at first time.

### Vendored (inlined) dependencies

Included the dependency source code directly in `contrib` directory, It depends to include target dependency as git submodule or include source code directly for each dependency respectively, for example by source file number, upstream status. Discussion may necessary.

### Dependencies policy

- Consider full-offline building, avoid cmake `FetchContent` module in case download dependency at configure time

