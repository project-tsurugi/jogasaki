# Copilot Instructions

## code locations

- public header files: `include/jogasaki` directory
- main code: `src/jogasaki` directory
- test code: `test/jogasaki` directory
- sample code: `example/jogasaki` directory
- mock classes for test and sample code: `mock` directory

## external dependencies

### build directory

- building the project and running tests should be done in a separate build directory create at the root of the repository.

- typical build directory name:

`build-<build_type>-<sharksfin_implementation>`

Here `<build_type>` is one of `debug`, `relwithdebinfo` or `release`. `<sharksfin_implementation>` is one of `shirakami` or `memory`.

- for most of the development work, debug build with shirakami implementation (i.e. `build-debug-shirakami`) is recommended since it provides full functionality and better debugging experience.
- for quick testing without shirakami functionality, you can use `memory` sharksfin implementation (e.g. `build-debug-memory`), which is faster to build and run but has some limitations
- for performance testing, `relwithdebinfo` build can be used (e.g. `build-relwithdebinfo-shirakami`)

### install directory

- Following are Tsurugi components that jogasaki depends
    - takatori
    - yugawara
    - mizugaki
    - tateyama
    - sharksfin
    - shirakami
    - yakushima
    - limestone
- Header files for these dependencies are installed under `${HOME}/git/.local-debug/include/${component_name}/`. Here `${component_name}` is one of the actual component names above.
- The compiler uses `-isystem ${HOME}/git/.local-debug/include/${component_name}` as the include search path, so an `#include` directive maps directly to a file path by prepending the search path. For example:
  - `#include <takatori/plan/group.h>` → `${HOME}/git/.local-debug/include/takatori/takatori/plan/group.h`
  - `#include <yugawara/aggregate/declaration.h>` → `${HOME}/git/.local-debug/include/yugawara/yugawara/aggregate/declaration.h`
- refer to `compile_commands.json` in the build directory for the exact include paths used in the project.

## Coding Conventions

- any code must conform to C++17 standard
- use modern C++ idioms and best practices
- Write code comments in English.
- use doxygen-style comments for functions, and classes.
  - `@brief` is required for all functions and classes.
  - `@param` and `@return` should be specified where applicable.
- enum class should always be defined with `to_string_view` function and `operator<<` for `std::ostream` to support logging. (refer to `src/jogasaki/auth/action_kind.h` for example)
  - `to_string_view` should return the string representation of the enum value.
- when applicable, class should be defined with the following functions (refer to `src/jogasaki/auth/action_set.h` for example)
  - `operator<<` for `std::ostream` to support logging.
  - `operator==` and `operator!=` to support comparison if applicable.

- at the top of each .h file, specify `#pragma once` to avoid multiple inclusion.

- Code formatting (strict, follow consistently):

  - Indentation: 4 spaces, no tabs.
  - Namespace: use `namespace a::b { ... }` syntax (no nested blocks).
  - Braces:
    - Function, class, struct, namespace: opening brace on the same line.
    - Always use braces `{}` even for single-line if/else/for/while.
  - Line length: ≤ 100 chars. Break long expressions onto multiple lines with +4 space continuation indent.
  - Include order:
    1. Corresponding header (if any)
      - Example: foo.cpp must start with #include <jogasaki/.../foo.h>
    2. C++ standard library
    3. External dependencies (Tsurugi components)
    4. Other jogasaki headers
    5. Local/private headers
  - Blank lines:
    - 1 blank line between functions.
  - File ending: exactly one newline, no trailing spaces.

  - Always place `const` after the type:
    - Use `A const&` instead of `const A&`.
    - Use `A const*` instead of `const A*`.
  - When using the logical NOT operator, always put a space between `!` and the condition:
    - Write `! condition`, not `!condition`.

- Copilot must always output code in this format unless explicitly instructed otherwise.


- Always use `#include <...>` for all headers:
  - standard library
  - external dependencies
  - jogasaki project headers (both public and internal, including test/mock/example)
- Do not use `#include "..."` in this project.

- Naming rules (strict):
  - Use `snake_case` for **all identifiers**: variables, functions, methods, classes, structs, enums, enum members, constants, and macros.
  - The only exception is template parameters, which must use `CamelCase` (e.g., `typename T`, `typename ValueType`).
  - Do not use `UPPER_CASE` or `CamelCase` for enum members, constants, or macros in this project. They are also `snake_case`.

  - Examples:
    - variable: `row_count`
    - function: `parse_record()`
    - class: `row_reader`
    - enum class: `action_kind { read_only, read_write }`
    - macro: `max_buffer_size`
    - template: `template<typename ValueType>`

- postfix member variables with `_` (underscore).
- where applicable, add `{}` to create new default-initialized variables
- avoid functions defined in header files except for template functions and very short functions (e.g., getters).

- In all test code, always write negative conditions with `EXPECT_TRUE(! condition)` or `ASSERT_TRUE(! condition)`.
- Do not use `EXPECT_FALSE(...)` or `ASSERT_FALSE(...)` anywhere in this project.


- License header policy:

  - New files (.h/.cpp): insert the header exactly as below and set the end year to the system current year.
```
/*
 * Copyright 2018-<CURRENT_YEAR> Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
```

  - Existing files: DO NOT change the header or the year range unless explicitly asked to “update license year”.
    (No automatic bumping.)

  - Code snippets in conversations or partial fragments: omit the license header.

- Protocol Buffers (oneof handling):

  - Do not rely on `has_foo()` accessors for oneof fields, since older versions of protoc do not generate them.
  - Instead, define project-level wrapper functions with the signature:
      bool has_foo(MyMessage const& msg);
  - The wrapper must internally check the `*_opt_case()` method of the oneof and return true/false.
  - All project code must call these wrapper functions, not `*_opt_case()` directly.
  - This provides a stable API so that, if newer protobuf versions add `has_foo()`, only the wrapper implementation needs to change, not all call sites.

- clang-tidy policy:

  - Code generated by Copilot must compile **without any clang-tidy warnings** under the project's configuration.
  - The following checks are enabled and enforced (treat them as errors):
    - modernize-use-nullptr
    - modernize-use-override
    - modernize-loop-convert
    - modernize-avoid-c-arrays
    - modernize-deprecated-headers
    - performance-for-range-copy
    - readability-implicit-bool-conversion
    - readability-inconsistent-declaration-parameter-name
    - readability-qualified-auto
    - readability-redundant-member-init
    - readability-redundant-smartptr-get
    - readability-simplify-boolean-expr

  - Copilot must always:
    - Prefer modern idioms (`nullptr`, range-for, smart pointers).
    - Always mark overridden virtuals with `override`.
    - Avoid raw pointers, C arrays, and deprecated headers.
    - Never call `.get()` on smart pointers unnecessarily.
    - Simplify boolean expressions and make casts explicit.

  - Do not guess about which rules are optional: **all listed checks are mandatory**.
  - If Copilot cannot generate code that satisfies these checks, insert a `// TODO: fix clang-tidy warning` marker rather than violating the rules.
