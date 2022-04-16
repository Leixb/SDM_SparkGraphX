{

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

    flake-utils = {
      url = "github:numtide/flake-utils";
      inputs.nixpkgs.follows = "nixpkgs";
    };

  };
  outputs = { self, nixpkgs, flake-utils, mach-nix, ...}:

  flake-utils.lib.eachDefaultSystem (system:
  let
    pkgs = import nixpkgs { inherit system; };
  in
  rec {
    devShell = with pkgs; mkShellNoCC {
      buildInputs = [
        jdk8 
        java-language-server 
      ];
    };

    packages = {
      jar = pkgs.callPackage ./.jar.nix { };
    };

    defaultPackage = packages.jar;
  });
}
