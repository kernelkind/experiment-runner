{
  description = "Shared experiment runner";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/0954f7ee2f6bb3dc7d4e3d0d8bcb8fd4bde4cfc5";
  outputs = { self, nixpkgs }: let
    systems = [ "x86_64-linux" "aarch64-linux" ];
    eachSystem = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
  in {
    packages = eachSystem (pkgs: { default = pkgs.callPackage ./package.nix { }; });
    apps = eachSystem (pkgs: {
      default = { type = "app"; program = "${self.packages.${pkgs.stdenv.hostPlatform.system}.default}/bin/experiment-runner"; };
    });
  };
}
