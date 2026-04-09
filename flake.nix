{
  description = "slatedb dev shell (scoped to the embedded-compactor -> memtable flusher notify feature)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs { inherit system overlays; };

        # Pinned to match rust-toolchain.toml (channel = "1.91.1").
        rustToolchain = pkgs.rust-bin.fromRustupToolchainFile
          ./rust-toolchain.toml;
      in
      {
        devShells.default = pkgs.mkShell {
          packages = [
            rustToolchain
            pkgs.pkg-config
            pkgs.protobuf
            pkgs.cmake
          ] ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [
            pkgs.libiconv
          ];

          env = {
            RUST_BACKTRACE = "1";
          };
        };
      });
}
