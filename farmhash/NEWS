FarmHash v1.1, March 1, 2015

  * Added a new 64-bit hash function (namespace farmhashte) that is
    substantially faster for long strings.  It requires SSE4.1.  If
    AVX instructions are available then it should be slightly faster (e.g.,
    use g++ -mavx).
  * Added a 32-bit hash function trivially derived from the above.  Useful
    on CPUs with SSE4.1 or AVX.
  * Added new 64-bit hash functions derived from FarmHash v1.0
    (namespaces farmhashuo and farmhashxo).  My testing suggests that for
    some string lengths these are speedier than the old standby (farmhashna).
  * Compiling with FARMHASH_NO_BUILTIN_EXPECT defined will now eliminate
    FarmHash's usage of __builtin_expect.  Thanks to Cory Riddell for
    suggesting this.
  * Improved some comments, the README, etc.

FarmHash v1.0, March 31, 2014

  * Initial release
