#ifndef FARMHASH_SELF_TEST_GUARD
#define FARMHASH_SELF_TEST_GUARD
#include <cstdio>
#include <iostream>
#include <string.h>

using std::cout;
using std::cerr;
using std::endl;
using std::hex;

static const uint64_t kSeed0 = 1234567;
static const uint64_t kSeed1 = k0;
static const int kDataSize = 1 << 20;
static const int kTestSize = 300;
#define kSeed128 Uint128(kSeed0, kSeed1)

static char data[kDataSize];

static int completed_self_tests = 0;
static int errors = 0;

// Initialize data to pseudorandom values.
void Setup() {
  if (completed_self_tests == 0) {
    uint64_t a = 9;
    uint64_t b = 777;
    for (int i = 0; i < kDataSize; i++) {
      a += b;
      b += a;
      a = (a ^ (a >> 41)) * k0;
      b = (b ^ (b >> 41)) * k0 + i;
      uint8_t u = b >> 37;
      memcpy(data + i, &u, 1);  // uint8_t -> char
    }
  }
}

int NoteErrors() {
#define NUM_SELF_TESTS 0
  if (++completed_self_tests == NUM_SELF_TESTS)
    std::exit(errors > 0);
  return errors;
}

template <typename T> inline bool IsNonZero(T x) {
  return x != 0;
}

template <> inline bool IsNonZero<uint128_t>(uint128_t x) {
  return x != Uint128(0, 0);
}

#endif  // FARMHASH_SELF_TEST_GUARD

namespace SKELETON {

uint32_t CreateSeed(int offset, int salt) {
  uint32_t h = static_cast<uint32_t>(salt & 0xffffffff);
  h = h * c1;
  h ^= (h >> 17);
  h = h * c1;
  h ^= (h >> 17);
  h = h * c1;
  h ^= (h >> 17);
  h += static_cast<uint32_t>(offset & 0xffffffff);
  h = h * c1;
  h ^= (h >> 17);
  h = h * c1;
  h ^= (h >> 17);
  h = h * c1;
  h ^= (h >> 17);
  return h;
}

#undef SEED
#undef SEED1
#undef SEED0
#define SEED CreateSeed(offset, -1)
#define SEED0 CreateSeed(offset, 0)
#define SEED1 CreateSeed(offset, 1)

#undef TESTING
#define TESTING 0
#if TESTING

// Return false only if offset is -1 and a spot check of 3 hashes all yield 0.
bool Test(int offset, int len = 0) {
#undef Check
#undef IsAlive

#define Check(x) do {                                                   \
  const uint32_t actual = (x), e = expected[index++];                   \
  bool ok = actual == e;                                                \
  if (!ok) {                                                            \
    cerr << "expected " << hex << e << " but got " << actual << endl;   \
    ++errors;                                                           \
  }                                                                     \
  assert(ok);                                                           \
} while (0)

#define IsAlive(x) do { alive += IsNonZero(x); } while (0)

  // After the following line is where the uses of "Check" and such will go.
  static int index = 0;

  return true;
#undef Check
#undef IsAlive
}

int RunTest() {
  Setup();
  int i = 0;
  cout << "Running SKELETON";
  if (!Test(-1)) {
    cout << "... Unavailable\n";
    return NoteErrors();
  }
  // Good.  The function is attempting to hash, so run the full test.
  int errors_prior_to_test = errors;
  for ( ; i < kTestSize - 1; i++) {
    Test(i * i, i);
  }
  for ( ; i < kDataSize; i += i / 7) {
    Test(0, i);
  }
  Test(0, kDataSize);
  cout << (errors == errors_prior_to_test ? "... OK\n" : "... Failed\n");
  return NoteErrors();
}

#else

// After the following line is where the code to print hash codes will go.
void Dump(int offset, int len) {
}

#endif

#undef SEED
#undef SEED1
#undef SEED0

}  // namespace SKELETON

#if !TESTING
int main(int argc, char** argv) {
  Setup();
  cout << "uint32_t expected[] = {\n";
  int i = 0;
  for ( ; i < kTestSize - 1; i++) {
    SKELETON::Dump(i * i, i);
  }
  for ( ; i < kDataSize; i += i / 7) {
    SKELETON::Dump(0, i);
  }
  SKELETON::Dump(0, kDataSize);
  cout << "};\n";
}
#endif
