// Copyright (c) 2014 Google, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
//
// FarmHash, by Geoff Pike

#include <cstdio>
#include <unordered_map>
#include <unordered_set>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <assert.h>
#include <stdlib.h>
#include <string.h>

static bool debug = false;

using namespace std;

const char* smhasher_dir = "/var/tmp/smhasher";
const char* get_smhasher = "svn checkout -r151 http://smhasher.googlecode.com/svn/trunk/";
const char* default_parallelism = "16";

string dir;
string parallelism;

int System(const string& cmd) {
  if (debug) cerr << cmd << '\n';
  return system(cmd.c_str());
}

string itoa(int n) {
  char buf[20];
  sprintf(buf, "%d", n);
  return string(buf);
}

string ToUpper(const string& s) {
  string result(s);
  for (int i = 0; i < result.size(); i++) {
    if (islower(result[i])) result[i] += 'A' - 'a';
  }
  return result;
}

void DoWithNoOutputExpected(const string& s) {
  static int counter = 0;
  cout << s << '\n';
  const string o = dir + "/e_" + itoa(counter++);
  string cmd = "(" + s + ")>&" + o;
  const int r = System(cmd);
  const bool success = r == 0;
  cmd = "cmp -s /dev/null " + o;
  const bool no_output = System(cmd) == 0;
  if (no_output && success) return;
  if (!no_output) {
    cerr << "Expected no output from '" << s << "' but got:";
    cmd = "cat " + o;
    const int q = System(cmd);
    assert(q == 0);
  }
  if (r != 0) {
    cerr << "Expected exit code 0 from '" << s << "' but got " << r << '\n';
  }
  abort();
}

vector<string> FileContents(const string& path) {
  fstream f;
  vector<string> result(1);
  f.open(path.c_str(), std::fstream::in);
  while (f.good()) {
    char c = f.get();
    if (!f.good()) break;
    if (c == '\n') {
      result.push_back("");
    } else {
      result.back().push_back(c);
    }
  }
  f.close();
  if (result.back().empty()) result.resize(result.size() - 1);
  return result;
}

vector<string> DoAndCollectOutput(const string& s) {
  static int counter = 0;
  cout << s << '\n';
  string o = dir + "/o_" + itoa(counter++);
  string cmd = "(" + s + ")>&" + o;
  int r = System(cmd);
  if (r != 0) cout << "exit code: " << r << '\n';
  return FileContents(o);
}

void DoAndIgnoreOutput(const string& s) {
  static int counter = 0;
  cout << s << '\n';
  string o = dir + "/i_" + itoa(counter++);
  string cmd = "(" + s + ")>&" + o;
  int r = System(cmd);
  if (r != 0) cout << "exit code: " << r << '\n';
}

void DoAndShowOutput(const string& s) {
  for (const string& l : DoAndCollectOutput(s)) cout << l << '\n';
}

string FindFarm(const string& s) {
  if (s.find("decl") == string::npos) return "";
  if (s.find(" not ") == string::npos) return "";
  if (s.find("farmhash") == string::npos) return "";
  for (int i = 0; i < s.size() - 10; i++) {
    if (string(s.c_str() + i, s.c_str() + i + 8) == "farmhash" &&
        isalpha(s[i+8]) &&
        isalpha(s[i+9])) {
      return string(s.c_str() + i, s.c_str() + i + 10);
    }
  }
  return "";
}

void AppendFileToFile(const string& f0, const string& f1) {
  DoWithNoOutputExpected("cat " + f0 + " >> " + f1);
}

void AppendToFile(const string& s, const string& filename) {
  fstream f;
  f.open(filename.c_str(), std::fstream::out | std::fstream::app);
  f << s;
  f.close();
}

void CreateTestBoilerplate(const string& inputfile, const string& boilerplate) {
  vector<string> files_to_use;
  static const char* consider[] = {
    "Hash32", "Hash32WithSeed",
    "Hash64", "Hash64WithSeed", "Hash64WithSeeds" };
  for (const char* c : consider) {
    string cmd = "fgrep '" + string(c) + "(const char' " + inputfile +
        " > /dev/null";
    if (System(cmd) == 0) files_to_use.push_back(c);
  }
  DoWithNoOutputExpected("touch " + boilerplate);
  if (!files_to_use.empty()) {
    AppendToFile("#if FARMHASH_TEST\n", boilerplate);
    for (const auto& f : files_to_use) {
      AppendFileToFile("TESTBOILERPLATE" + f, boilerplate);
    }
    AppendToFile("#endif  // FARMHASH_TEST\n", boilerplate);
  }
}

static unordered_map<string, vector<string>>* deps = NULL;

static int files_included = 0;

// We need the code in the given namespace.  If it depends on code
// in other namespaces then process those first.  Do nothing if
// the given namespace has already been handled by this function.
void IncludeCode(const string& name) {
  static unordered_set<string>* started = NULL;
  static unordered_set<string>* finished = NULL;
  if (finished == NULL) {
    finished = new unordered_set<string>();
    started = new unordered_set<string>();
  }
  if (started->count(name) > finished->count(name)) {
    cerr << "ERROR: Circular dependence involving " << name << "!\n";
    abort();
  }
  if (started->insert(name).second) {
    for (const string& dep : (*deps)[name]) IncludeCode(dep);
    cout << "Include " << name << '\n';
    string inputfile = name + ".cc";
    string outputfile = dir + "/" + name + "_gen.cc";
    string boilerplate = dir + "/" + name + "_gen_test_boilerplate.cc";
    CreateTestBoilerplate(inputfile, boilerplate);
    DoWithNoOutputExpected("(echo 'namespace " + name + " {'; "
                           "cat " + inputfile + " " + boilerplate + "; "
                           "echo '}  // namespace " + name + "')>&" +
                           outputfile + " && chmod 444 " + outputfile);
    finished->insert(name);
    ++files_included;
  }
}

vector<string> Split(const string& s, char delim) {
  vector<string> result(1);
  for (int i = 0; i < s.size(); i++) {
    char c = s[i];
    if (c == delim) {
      result.push_back("");
    } else {
      result.back().push_back(c);
    }
  }
  if (result.back().empty()) result.resize(result.size() - 1);
  return result;
}

void CheckForDir(const string& dir) {
  DoWithNoOutputExpected("test -d " + dir + " || echo missing directory");
}

// For example, from "xu32_871q:" the result is 32.
string GetFirstNumeral(const string& s) {
  int i = 0;
  do {
    assert(i < s.size());
    if (isdigit(s[i])) break;
  } while (++i);
  int j = i + 1;
  while (j < s.size() && isdigit(s[j])) ++j;
  return string(s.c_str() + i, s.c_str() + j);
}

void ModifySMHasherForTest(const string& nspace,
                           const string& fn,
                           const string& testdir) {
  IncludeCode(nspace);
  // For the calculation of v, use an explicit temporary file to avoid
  // "fgrep: write error: Broken pipe" errors.
  const string tmp = testdir + "/tmp";
  vector<string> v =
      DoAndCollectOutput("cat " + dir + "/" + nspace + "*.cc | "
                         "fgrep -v -i static | "
                         "fgrep '{' > " + tmp +
                         " && "
                         "fgrep --max-count=1 ' " + fn + "(const char' " + tmp +
                         " && " +
                         "rm " + tmp);
  assert(v.size() == 1);
  const string defline = v[0];
  if (defline[defline.size() - 1] != '{') {
    cerr << "Line defining " << fn << " should end in '{'" << '\n';
    abort();
  }
  // Get summary of declared return type and args
  v = DoAndCollectOutput("sed s/" + fn + "//<<<'" + defline + "' "
                         "| tr -cd a-z0-9");
  assert(v.size() == 1);
  const string bits = GetFirstNumeral(v[0]);
  // Create wrapper function
  const string wrapper = testdir + "/wrapper.cc";
  DoWithNoOutputExpected("sed s/FUNCTION/" + nspace + "::" + fn + "/g"
                         " < WRAPPER" + v[0] + " > " + wrapper);
  const string testfile = nspace + "_test.cc";
  // Put relevant farmhash code and wrapper function together in one file, and
  // include that file in SMHasher's main.cpp.
  DoWithNoOutputExpected("echo '#define FARMHASH_TEST 1' | "
                         "cat - farmhash.h platform.cc basics.cc > " +
                         testdir + "/" + testfile);
  DoWithNoOutputExpected("ls -tr " + dir + "/*_gen.cc | "
                         "xargs cat >> " + testdir + "/" + testfile);
  DoWithNoOutputExpected("cat f.cc " + wrapper + " >> " + testdir + "/" + testfile);
  DoWithNoOutputExpected("cd " + testdir + " && "
                         "sed -i '/include \"Platform.h\"/a "
                         "#include \"" + testfile + "\"' main.cpp");
  // Hook up wrapper function so main.cpp can invoke it.
  const string q = "\"" + nspace + "_" + fn + "\"";
  DoWithNoOutputExpected("cd " + testdir + " && "
                         "sed -i '/3719DB20/a "
                         "  { WRAPPER, " + bits + ", 0/*verification code*/, " +
                         q + ", " + q + " },"
                         "' main.cpp");
  // Add a special-case to SMHasher for when the expected
  // "verification value" is zero:  If the computed verification value is
  // non-zero but the expected value is zero then don't report an error.
  DoWithNoOutputExpected("cd " + testdir + " && "
                         "sed -i 's/\\(.*print.*Verification value.*Failed\\)/"
                         "    if(expected == 0) return true;\\1/' KeysetTest.cpp");
  // Modify CMakeLists.txt if needed
  char* t = getenv("CMAKE32");
  if (t != NULL && t[0] != '\0' && t[0] != '0') {
    string f = testdir + "/CMakeLists.txt";
    string ftmp = f + ".tmp";
    DoWithNoOutputExpected("cat cmake_m32 " + f + " > " + ftmp + " && "
                           "mv " + ftmp + " " + f);
  }
}

void CreateTestList(const string& test, const string& path) {
  assert(test.size() > 12);
  assert(string(test.c_str(), test.c_str() + 8) == "farmhash");
  assert(isalpha(test[8]) && isalpha(test[9]));
  assert(test[10] == ':' && test[11] == ':');
  const string nspace(test.c_str(), test.c_str() + 10);
  const string fn(test.c_str() + 12);

  static int counter = 0;
  const string testdir = path + itoa(counter++);
  DoWithNoOutputExpected("cp -R " + string(smhasher_dir) + " " + dir);
  DoWithNoOutputExpected("cd " + dir + " && "
                         "mv `basename " + smhasher_dir + "` " + testdir);
  DoAndShowOutput("cat PATCH | (cd " + testdir + " && patch) "
                  "|| echo patch failed");
  DoAndShowOutput("cd " + testdir + " && "
                  "for i in *.cpp *.h; "
                  "  do sed -i s/uint128_t/blob128/g $i; "
                  "done");
  ModifySMHasherForTest(nspace, fn, testdir);
  DoAndShowOutput("(cd " + testdir + " && cmake . && make -j" + parallelism + " VERBOSE=1) "
                  "|| echo building smhasher failed");
  const int kParts = 10;
  fstream f;
  f.open(path.c_str(), std::fstream::out | std::fstream::app);
  for (int i = 0; i < kParts; i++) {
    f << testdir << "/SMHasher --noaffinity --part" << i
      << " " << nspace << "_" << fn << " >& "
      << testdir << "/part" << i << '\n';
  }
  f.close();
}

void Test(const string& tests) {
  string parent = string("`dirname ") + smhasher_dir + "`";
  string basename = string("`basename ") + smhasher_dir + "`";
  CheckForDir(parent);
  DoAndIgnoreOutput("cd " + parent + "; test -d " + smhasher_dir + " || " +
                    get_smhasher + " " + basename);
  CheckForDir(smhasher_dir);
  vector<string> v = Split(tests, ',');
  string testsfile = dir + "/tests";
  for (const string& s : v) CreateTestList(s, testsfile);
  DoAndShowOutput(string("./do-in-parallel -k ") + parallelism +
                  " " + testsfile + " || echo FAILED");
  DoAndShowOutput("grep -B 9 -i fail " + dir + "/test*/part* || echo nothing");
  cout << "\nSummary of '!!!!!' and tests with expected number of collisions in [0.1, 1):\n\n";
  DoAndShowOutput("egrep 'collisions.*Expected.* 0[.][1-9].*, actual|!!!!!' " + dir + "/test*/part*");
}

// Note the cases where, for example something in farmhashxy calls something in
// farmhashqq.  Circular deps are disallowed.
void ComputeDeps() {
  assert(deps == NULL);
  deps = new unordered_map<string, vector<string>>(5);
  for (const string& line : DoAndCollectOutput("grep farmhash..:: *.cc")) {
    assert(line.find(":") != string::npos);  // line should begin with filename
    if (line.find("::") == string::npos) continue;
    int c = line.find(":");
    for (int i = c; i < line.size() - 12; i++) {
      if (string(line.data() + i, line.data() + i + 8) == "farmhash" &&
          isalpha(line[i+8]) && isalpha(line[i+9]) &&
          line[i+10] == ':' && line[i+11] == ':') {
        assert(string(line.data() + c - 3, line.data() + c) == ".cc");
        string from(line.data(), line.data() + c - 3);
        string to(line.data() + i, line.data() + i + 10);
        cout << "Found usage of " << to << " in " << from << '\n';
        (*deps)[from].push_back(to);
      }
    }
  }
}

int main(int argc, char** argv) {
  // Step 1: What directory are we going to use?  Assume it is relative to /tmp.
  cout << "Step 1\n";
  const char* d = getenv("DIR");
  assert(d != NULL && d[0] != '\0' && d[0] != '/');
  dir = string("/tmp/") + d;
  assert(dir.find(" ") == string::npos);
  string cmd = "rm -rf " + dir + " && mkdir " + dir;
  int r = System(cmd);
  assert(r == 0);
  const char* p = getenv("PARALLELISM");
  parallelism = string(p == NULL ? default_parallelism : p);

  // Step 2: Create naive version of the code to which farmhash functions need
  // to be added.
  cout << "Step 2\n";
  string src = dir + "/a.cc";
  DoWithNoOutputExpected("cat - platform.cc basics.cc f.cc <<<"
                         "'#include \"farmhash.h\"' > " + src);
  DoWithNoOutputExpected("cp farmhash.h " + dir);
  DoWithNoOutputExpected("cd " + dir + " && chmod 444 farmhash.h " + src);
  vector<string> v =
      DoAndCollectOutput("cd " + dir + " && g++ -c -fmax-errors=9999 " + src);

  // Step 3: Create final version of the code.
  cout << "Step 3\n";
  //for (const string& s : v) cout << s << '\n';

  ComputeDeps();
  for (const string& s : v) {
    string name = FindFarm(s);
    if (!name.empty()) {
      IncludeCode(name);
    }
  }
  src = dir + "/b.cc";
  DoWithNoOutputExpected("cat <<<'#include \"farmhash.h\"' > " + src);
  DoWithNoOutputExpected("ls -tr " + dir + "/*_gen.cc | "
                         "xargs cat platform.cc basics.cc >> " + src);
  AppendFileToFile("f.cc", src);

  DoWithNoOutputExpected("cd " + dir + " && g++ -c " + src);
  DoWithNoOutputExpected("cd " + dir + " && g++ -m32 -c " + src);
  DoWithNoOutputExpected("cd " + dir + " && g++ -O3 -c " + src);
  DoWithNoOutputExpected("cd " + dir + " && g++ -m32 -O3 -c " + src);
  const char* build_flag_tests = getenv("BUILD_FLAG_TESTS");
  if (build_flag_tests != NULL) {
    string z = build_flag_tests;
    int b = 0, e = z.size();
    while (b < e) {
      int c = b;
      while (c < e && z[c] != '|') {
        ++c;
      }
      string flags(z.data() + b, z.data() + c);
      DoWithNoOutputExpected("cd " + dir + " && g++ " + flags + " -c " + src);
      b = c + 1;
    }
  }
  // Copy files to ../src
  DoWithNoOutputExpected("cp -f farmhash.h " + src + " ../src && "
                         "mv ../src/$(basename " + src + ") ../src/farmhash.cc");
  // Strip #if FARMHASH_TEST stuff
  assert(files_included > 0);
  for (int i = 0; i < files_included; i++) {
    DoWithNoOutputExpected("./remove-from-to 'if FARMHASH_TEST' 'endif.*FARMHASH_TEST' ../src/farmhash.cc");
  }
  // Fix copyright notices.
  DoAndShowOutput("./fix-copyright ../src/farmhash.h");
  DoAndShowOutput("./fix-copyright ../src/farmhash.cc");

  // Step 4: Generate self-test code
  cout << "Step 4\n";
  // TODO: what if I need multiple machines to run the various bits of platform-specific code?
  string m = dir + "/m.cc";
  DoAndShowOutput("count=$(ls " + dir + "/*_gen.cc | wc -l); "
                  "for i in " + dir + "/*_gen.cc; "
                  "do"
                  " f=${i%_gen.cc}_selftest0.cc; ./create-self-test $i $f &&"
                  " pushd " + dir + " && echo $i &&"
                  " cat " + src + " $f > " + m + " &&"
                  " g++ -maes -msse4.2 -msse4.1 -mssse3 " + m + " && ./a.out > tmp.cc && popd &&"
                  " l=$(fgrep -n 'if TESTING' $f | head -1 | cut -f 1 -d :) &&"
                  " (head -n $l $f | sed -e 's/define TESTING 0/define TESTING 1/' -e \"s/define NUM_SELF_TESTS 0/define NUM_SELF_TESTS $count/\"; cat " + dir + "/tmp.cc;"
                  " tail -n +$((l + 1)) $f) > ${i%_gen.cc}_selftest1.cc; "
                  "done");
  DoWithNoOutputExpected("(echo; echo '#if FARMHASHSELFTEST'; echo;"
                         " cat " + dir + "/*_selftest1.cc;"
                         " echo; echo 'int main() {';"
                         " for i in " + dir + "/*_gen.cc; "
                         " do"
                         "   namespace=$(basename ${i%_gen.cc});"
                         "   echo '  '${namespace}'Test::RunTest();';"
                         " done;"
                         " echo '  __builtin_unreachable();';"
                         " echo '}';"
                         " echo; echo '#endif  // FARMHASHSELFTEST') "
                         ">> ../src/farmhash.cc");

  // Step 5: Quality testing
  d = getenv("TEST");
  if (d != NULL && d[0] != '\0') {
    cout << "Step 5\n";
    Test(d);
  }
  return 0;
}
