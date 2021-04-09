---
title: com::_4paradigm::hybridse::LibraryLoader
summary: This class is used to load the shared library from within the jar. The shared library is extracted to a temp folder and loaded from there. 

---
# com::_4paradigm::hybridse::LibraryLoader



This class is used to load the shared library from within the jar. The shared library is extracted to a temp folder and loaded from there. 
## Summary


|  Public functions|            |
| -------------- | -------------- |
|**[loadLibrary](/hybridse/usage/api/c++/Classes/classcom_1_1__4paradigm_1_1hybridse_1_1_library_loader.md#function-loadlibrary)**(String libraryPath)| synchronized static void  |
|**[extractResource](/hybridse/usage/api/c++/Classes/classcom_1_1__4paradigm_1_1hybridse_1_1_library_loader.md#function-extractresource)**(String path, boolean isTemp)| String  |

## Public Functions

#### function loadLibrary

```cpp
static inline synchronized static void loadLibrary(
    String libraryPath
)
```


**Parameters**: 

  * **libraryPath** 


Firstly attempts to load the native library specified by the libraryPath. If that fails then it falls back to extracting the library from the classpath. 

#### function extractResource

```cpp
static inline String extractResource(
    String path,
    boolean isTemp
)
```


**Parameters**: 

  * **path** Local resource path 
  * **isTemp** If extract to template file 


**Exceptions**: 

  * **IOException** 


**Return**: 

Extract library in resource into filesystem 

