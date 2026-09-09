target "graviton" {
  context = "."
  dockerfile = "deploy/Dockerfile"
  tags = ["alto-validator-builder:graviton-local"]
  args = {
    TARGET_TRIPLE = "aarch64-unknown-linux-gnu"
    TARGET_CARGO_NAME = "AARCH64_UNKNOWN_LINUX_GNU"
    TARGET_CC_NAME = "aarch64_unknown_linux_gnu"
    TARGET_CPU = "neoverse-512tvb"
    TARGET_LINKER = "aarch64-linux-gnu-gcc"
    TARGET_CXX = "aarch64-linux-gnu-g++"
    TARGET_AR = "aarch64-linux-gnu-ar"
    TARGET_STRIP = "aarch64-linux-gnu-strip"
  }
}

target "graviton4" {
  inherits = ["graviton"]
  tags = ["alto-validator-builder:graviton4-local"]
  args = {
    TARGET_CPU = "neoverse-v2"
  }
}

target "intel" {
  context = "."
  dockerfile = "deploy/Dockerfile"
  tags = ["alto-validator-builder:intel-local"]
  args = {
    TARGET_TRIPLE = "x86_64-unknown-linux-gnu"
    TARGET_CARGO_NAME = "X86_64_UNKNOWN_LINUX_GNU"
    TARGET_CC_NAME = "x86_64_unknown_linux_gnu"
    TARGET_CPU = "emeraldrapids"
    TARGET_LINKER = "x86_64-linux-gnu-gcc"
    TARGET_CXX = "x86_64-linux-gnu-g++"
    TARGET_AR = "x86_64-linux-gnu-ar"
    TARGET_STRIP = "x86_64-linux-gnu-strip"
  }
}
