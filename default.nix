let
  pkgs = import <nixpkgs> {};
in pkgs.mkShell {
  buildInputs = with pkgs; [
    llvmPackages_12.libllvm
    llvmPackages_12.llvm
    llvmPackages_12.clang
    pkgconfig
    openssl
  ];
}
