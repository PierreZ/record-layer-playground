{
  description = "A Nix-flake-based Java development environment";

  inputs.nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
  inputs.fdb-overlay.url = "github:foundationdb-rs/overlay";
  inputs.fdb-overlay.inputs.nixpkgs.follows = "nixpkgs";

  outputs = { self, nixpkgs, fdb-overlay }:
    let
      javaVersion = 22; # Change this value to update the whole stack
      overlays = [ fdb-overlay.overlays.default ];

      supportedSystems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forEachSupportedSystem = f: nixpkgs.lib.genAttrs supportedSystems (system: f {
        pkgs = import nixpkgs {
          inherit overlays;
          inherit system;
        };
      });
    in
    {
      overlays.default =
        final: prev: rec {
          jdk = prev."jdk${toString javaVersion}";
          maven = prev.maven.override { jre = jdk; };
          gradle = prev.gradle.override { java = jdk; };
        };

      devShells = forEachSupportedSystem ({ pkgs }: {
        default = pkgs.mkShell {
          packages = with pkgs; [ jdk maven gradle gcc ncurses patchelf zlib protobuf_25 libfdb73 ];
          LD_LIBRARY_PATH = "${pkgs.libfdb73}/include";
        };
      });
    };
}
